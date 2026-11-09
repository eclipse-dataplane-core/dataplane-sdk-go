//  Copyright (c) 2025 Metaform Systems, Inc
//
//  This program and the accompanying materials are made available under the
//  terms of the Apache License, Version 2.0 which is available at
//  https://www.apache.org/licenses/LICENSE-2.0
//
//  SPDX-License-Identifier: Apache-2.0
//
//  Contributors:
//       Metaform Systems, Inc. - initial API and implementation
//

package natsservices

import (
	"errors"
	"fmt"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/common"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/logger"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/synadia-io/callout.go"
	"log"
	"time"
)

// AuthService implements a NATS authentication callout extension. For details, see:
// https://docs.nats.io/running-a-nats-service/configuration/securing_nats/auth_callout.
type AuthService struct {
	tokenStore  *common.Store[storeEntry]
	authService *callout.AuthorizationService
	nc          *nats.Conn
	accountKeys nkeys.KeyPair
}

func NewAuthService() *AuthService {
	accountKeys, _ := nkeys.CreateAccount()
	return &AuthService{tokenStore: common.NewStore[storeEntry](), accountKeys: accountKeys}
}

func (as *AuthService) InvalidateToken(processID string) error {
	// Remove from token store
	found := as.tokenStore.Delete(processID)
	if !found {
		return fmt.Errorf("token not found")
	}
	return nil
}

func (as *AuthService) Init() error {
	authKeyPair, _ := nkeys.FromSeed([]byte(authKP))

	authorizer := func(req *jwt.AuthorizationRequest) (string, error) {
		token := req.ConnectOptions.Token
		userClaims, err := jwt.DecodeUserClaims(token)
		if err != nil {
			log.Printf("Error decoding user JWT: %v\n", err)
		}

		// use the server-specified user nkey
		uc := jwt.NewUserClaims(req.UserNkey)

		// put the user in the global account
		uc.Audience = "$G"
		uc.Name = userClaims.Name // sets to processID
		uc.Permissions.Pub.Allow.Add(userClaims.Permissions.Pub.Allow...)
		uc.Permissions.Sub.Allow.Add(userClaims.Permissions.Sub.Allow...)

		uc.Expires = time.Now().Unix() + 90

		if as.tokenStore.Has(userClaims.Name) {
			return uc.Encode(authKeyPair)
		}
		return "", errors.New("not authorized")
	}

	// create a connection using the callout user
	nc, err := nats.Connect(NatsUrl, nats.UserInfo("auth", "pass"))
	if err != nil {
		log.Fatal("Failed to connect:", err)
	}
	as.nc = nc

	authLogger := logger.NewStdLogger(true, false, false, true, false)
	as.authService, err = callout.NewAuthorizationService(nc,
		callout.Authorizer(authorizer),
		callout.ResponseSignerKey(authKeyPair),
		callout.Logger(authLogger))

	if err != nil {
		return err
	}

	return nil
}

func (ns *AuthService) CreateToken(processID string, pull bool) (string, error) {
	userKeys, _ := nkeys.CreateUser()
	userPKey, _ := userKeys.PublicKey()

	userClaims := jwt.NewUserClaims(userPKey)
	userClaims.Name = processID

	// Restrict permissions
	if pull {
		userClaims.Permissions.Sub.Allow.Add(processID + "." + ForwardSuffix)
		userClaims.Permissions.Sub.Allow.Add(processID + "." + ReplySuffix)
	} else {
		userClaims.Permissions.Pub.Allow.Add(processID + "." + ForwardSuffix)
		userClaims.Permissions.Pub.Allow.Add(processID + "." + ReplySuffix)
	}

	userJWT, _ := userClaims.Encode(ns.accountKeys)
	ns.tokenStore.Create(processID, storeEntry{processID, userJWT})
	return userJWT, nil
}

func (ac *AuthService) shutdown() {
	if ac.nc != nil {
		ac.nc.Close()
	}
}

type storeEntry struct {
	processID string
	token     string
}
