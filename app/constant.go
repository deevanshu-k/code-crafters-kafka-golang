package main

type APIKey uint16

const (
	ApiVersionsAPIKey             APIKey = 18
	DescribeTopicPartitionsAPIKey APIKey = 75
)

func GetAPIKeyFromUint16(key uint16) APIKey {
	switch key {
	case 18:
		return ApiVersionsAPIKey
	case 75:
		return DescribeTopicPartitionsAPIKey
	default:
		return 0 // Unknown API Key
	}
}
