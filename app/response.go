package main

import (
	"encoding/binary"
	"fmt"
)

type ResponseHandler func([]byte) ([]byte, error)

var keyToResponseHandlers = map[APIKey]ResponseHandler{
	ApiVersionsAPIKey:             listAPIVersionHandler,   // ApiVersions
	DescribeTopicPartitionsAPIKey: describeTopicPartitions, // DescribeTopicPartitions
}

// RESPONSE MESSAGE
// - MESSAGE-SIZE: 4 bytes
//
// - RESPONSE-HEADER:
//   - CORRELATION-ID: 4 bytes
//
// - RESPONSE-BODY:
//   - ERROR-CODE: 2 bytes
//   - API-VERSIONS-ARRAY:
//     - ARRAY-LENGTH: 1 byte
//     - v1:
//       - API-KEY: 2 bytes
//       - MIN-VERSION: 2 bytes
//       - MAX-VERSION: 2 bytes
//       - TAGGED-FIELDS: 1 byte (probably 0x00)
//     - v2:
//   - THROTTLE-TIME: 4 bytes
//   - TAGGED-FIELDS: 1 byte (probably 0x00)

func listAPIVersionHandler(message []byte) (response []byte, err error) {
	apiVersion := int16(binary.BigEndian.Uint16(message[2:4]))    // Api Version
	correlationID := int32(binary.BigEndian.Uint32(message[4:8])) // Correlation ID

	errorCode := int16(0)
	if apiVersion < 0 || apiVersion > 4 {
		errorCode = 35 // UNSUPPORTED VERSION
	}

	response = binary.BigEndian.AppendUint32(response, uint32(correlationID)) // Correlation ID
	response = binary.BigEndian.AppendUint16(response, uint16(errorCode))     // 16 bytes: Error Code
	response = append(response, byte(3))                                      // 08 bytes: Api Versions Length

	// FOR APIVersions
	response = binary.BigEndian.AppendUint16(response, uint16(18)) // 16 bytes: Api Key
	response = binary.BigEndian.AppendUint16(response, uint16(3))  // 16 bytes: Min Version
	response = binary.BigEndian.AppendUint16(response, uint16(4))  // 16 bytes: Max Version
	response = append(response, byte(0))                           // 01 byte: Tagged Fields

	// For DescribeTopicPartitions
	response = binary.BigEndian.AppendUint16(response, uint16(75)) // 16 bytes: Api Key
	response = binary.BigEndian.AppendUint16(response, uint16(0))  // 16 bytes: Min Version
	response = binary.BigEndian.AppendUint16(response, uint16(0))  // 16 bytes: Max Version
	response = append(response, byte(0))                           // 01 byte: Tagged Fields

	response = binary.BigEndian.AppendUint32(response, uint32(0)) // 32 bytes: Throttle Time
	response = append(response, byte(0))                          // 01 byte: Tagged Fields

	return response, nil
}

// RESPONSE MESSAGE
// - MESSAGE-SIZE: 4 bytes
//
// - RESPONSE-HEADER:
//   - CORRELATION-ID: 4 bytes
//   - TAGGED-FIELDS: 1 byte (probably 0x00)
//
// - RESPONSE-BODY:
//   - THROTTLE-TIME: 4 bytes
//   - TOPICS-ARRAY:
//     - ARRAY-LENGTH: 1 byte
//     - TOPIC:
// 	     - ERROR-CODE: 2 bytes
//	     - TOPIC-NAME:
//         - LENGTH: 1 byte
//         - NAME: variable length (string)
//	     - TOPIC-ID: 16 bytes (UUID)
//	     - IS_INTERNAL: 1 byte (boolean)
//	     - PARTITIONS-ARRAY:
//         - ARRAY-LENGTH: 1 byte
//       - TOPIC-AUTHORIZED-OPERATIONS: 4 bytes (bitmask)
//       - TAGGED-FIELDS: 1 byte (probably 0x00)
//   - NEXT-CURSOR: 1 byte (probably 0x00)
//   - TAGGED-FIELDS: 1 byte (probably 0x00)

func describeTopicPartitions(message []byte) (response []byte, err error) {
	correlationID := int32(binary.BigEndian.Uint32(message[4:8])) // Correlation ID
	errorCode := int16(3)                                         // UNKNOWN_TOPIC_OR_PARTITION

	fmt.Println("> Describe Partition")

	// ---- Response header ---- //
	response = binary.BigEndian.AppendUint32(response, uint32(correlationID)) // Correlation ID
	response = append(response, byte(0))                                      // 01 byte: Tagged Fields

	// ---- Response body ---- //
	response = binary.BigEndian.AppendUint32(response, 0) // 32 bytes: Throttle Time

	clientIdLength := binary.BigEndian.Uint16(message[8:10]) // Client ID Length

	totalHeaderLength := 10 + clientIdLength + 1 // Total header length

	noOfTopics := int8(message[totalHeaderLength]) // 01 byte: Number of topics

	response = append(response, byte(noOfTopics)) // 01 byte: Topics Array Length

	offset := totalHeaderLength + 1 // Offset to start reading topics

	fmt.Printf("> Befor Loop\n   > Response: %x\n", response)

	for i := 0; i < int(noOfTopics-1); i++ {
		topicNameLength := uint8(message[offset])                       // 01 byte: Topic Name Length
		offset = offset + 1                                             // Move past the length byte
		topicName := message[offset : offset+uint16(topicNameLength-1)] // Topic Name
		offset = offset + uint16(topicNameLength-1)                     // Move past the topic name
		offset = offset + 1                                             // Skip Tagged Fields byte

		response = binary.BigEndian.AppendUint16(response, uint16(errorCode)) // 2 bytes: Error Code
		response = append(response, byte(topicNameLength))                    // 1 byte: Topic Name Length
		response = append(response, topicName...)                             // Topic Name
		response = append(response, make([]byte, 16)...)                      // 16 bytes: Topic ID (UUID)
		response = append(response, byte(0))                                  // 1 byte Is Internal
		response = append(response, byte(0x01))                               // 2 bytes Partition Array Length
		response = binary.BigEndian.AppendUint32(response, 0x00000df8)        // 4 bytes Topic Authorised Operations
		response = append(response, byte(0x00))                               // 1 bytes Tag Buffer
		fmt.Printf("> In Loop \n   > Response: %x\n", response)
	}

	fmt.Printf("> After Loop \n   > Response: %x\n", response)

	response = append(response, byte(0xff)) // 01 byte: next cursor

	response = append(response, byte(0)) // 01 byte: Tagged Fields

	return response, nil
}
