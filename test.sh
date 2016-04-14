#!/bin/sh
go test -coverprofile=coverage/cover.out catapult
go tool cover -html=coverage/cover.out -o coverage/index.html
