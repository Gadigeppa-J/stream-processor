package main

import (
	"encoding/json"
	"errors"
	"stream-processor/pipeline"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	// pipleline (source -> flow -> sink)

	pipe := pipeline.NewPipeline()
	source := pipeline.NewKafkaSource()
	sink := pipeline.NewPrintSink()

	mapFunction1 := func(in interface{}) (interface{}, error) {
		kafkaMsg := in.(*kafka.Message)
		str := string(kafkaMsg.Value)

		// conver Kafka record to map
		var result map[string]string
		json.Unmarshal([]byte(str), &result)

		if len(result) == 0 {
			return nil, errors.New("Unable to convert string to map")
			//fmt.Println("map in intialized!!. creating an empty map")
			//result = make(map[string]string)
		}

		return result, nil
	}

	/*
		mapFunction2 := func(in interface{}) interface{} {
			str := in.(string)
			str = fmt.Sprint(str, "-secondMap")
			return str
		}
	*/

	filterFunction := func(in interface{}) bool {
		blacklistOrgs := map[string]struct{}{"234": struct{}{}, "546": struct{}{}}

		msg := in.(map[string]string)
		orgID := msg["orgID"]

		if _, contains := blacklistOrgs[orgID]; contains {
			return false
		}
		return true
	}

	keyByFunction := func(in interface{}) string {
		msg := in.(map[string]string)
		orgID := msg["orgID"]
		return orgID
	}

	/*
		func (p *Process) processFunction(in interface{}) interface{}{
			msg := in.(map[string]string)
			msg["status"] = "processed"
			return msg
		}
	*/

	/*
		processFunction := func(in interface{}) interface{} {
			msg := in.(map[string]string)
			msg["status"] = "processed"
			return msg
		}
	*/

	pipe.Source(source).
		Map(mapFunction1).
		Filter(filterFunction).
		KeyBy(keyByFunction).
		Process(pipeline.NewIdentityProcessFuncFactory()).
		Sink(sink)

	pipe.Execute()

	/*
		pipe.source(kafkaSource).map()
		.filter()
		.keyby()
		.process()
		.sink()
	*/

	//pipe.execute()

}
