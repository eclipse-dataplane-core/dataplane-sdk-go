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
	"fmt"
	"github.com/nats-io/nats-server/v2/logger"
	"github.com/nats-io/nats-server/v2/server"
	"time"
)

const (
	authKP  = "SAAHZHKC43PG6B6EP3LZ7HB3HB3JD25GSJRV5LFZE2A6XFT57SDFRSEI4E"
	NatsUrl = "localhost:4222"
)

type NATSServer struct {
	server     *server.Server
	natsLogger *logger.Logger
}

func NewNatsServer() *NATSServer {
	return &NATSServer{}
}

func (ns *NATSServer) Init() error {
	opts := &server.Options{
		ServerName: "provider_nats",
		DontListen: false,
		JetStream:  false,

		Debug:   false,
		Trace:   false,
		Logtime: true,

		Users: []*server.User{
			{Username: "auth", Password: "pass"},
			{Username: "provider", Password: "provider"},
		},

		AuthCallout: &server.AuthCallout{
			AuthUsers: []string{"auth", "provider"},
			Issuer:    "AAB35RZ7HJSICG7D4IGPYO3CTQPWWGULJXZYD45QAWUKTXJYDI6EO7MV",
		},
	}

	var err error
	ns.server, err = server.NewServer(opts)
	if err != nil {
		return err
	}

	ns.natsLogger = logger.NewStdLogger(true, false, false, true, false)
	ns.server.SetLoggerV2(ns.natsLogger, false, false, false)

	go ns.server.Start()

	if !ns.server.ReadyForConnections(5 * time.Second) {
		return fmt.Errorf("server not ready for connections")
	}
	return nil
}

func (ns *NATSServer) Shutdown() {
	if ns.server == nil {
		return
	}
	ns.server.Shutdown()
}

func (ns *NATSServer) InvalidateConnection(dataFlowID string) {
	conns, err := ns.server.Connz(&server.ConnzOptions{Username: true})
	if err != nil {
		return
	}
	var id uint64
	for _, conn := range conns.Conns {
		if conn.AuthorizedUser == dataFlowID {
			id = conn.Cid
			break
		}
	}
	if id > 0 {
		err = ns.server.DisconnectClientByID(id)
		if err != nil {
			ns.natsLogger.Errorf("[NATS] Error disconnecting invalidated client: %v", err)
		}
		ns.natsLogger.Noticef("[NATS] Disconnected invalidated client")
	}
}
