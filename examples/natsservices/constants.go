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

const (
	NATSEndpointType = "https://test.org/natsdp/v1/nats"
	// PullProfile and PushProfile are the transfer profiles of the streaming samples; the SDK derives the transfer
	// type from the last '-' token.
	PullProfile     = "https://test.org/natsdp/v1/nats-pull"
	PushProfile     = "https://test.org/natsdp/v1/nats-push"
	TokenKey        = "token"
	ForwardSuffix   = "forward"
	ReplySuffix     = "reply"
	ChannelKey      = "channel"
	ReplyChannelKey = "replyChannel"
)
