package mapreduce

import (
	"log"
	"os"
	"encoding/json"
	"io"
	"fmt"

)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//

	// the input is a list of key->value pairs
	// read files and store them in map[string][]string
	m := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTaskNumber)
		f, err := os.Open(fileName)
		defer f.Close()
		if err != nil {
			log.Fatal(err)
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
				log.Fatal(err)
			}
			fmt.Println("libochen: ", kv.Key, " ", kv.Value)
			if kv.Value == "" {
				fmt.Println("equal")
			}
			m[kv.Key] = append(m[kv.Key], kv.Value)
			fmt.Println("want to debug: ", m[kv.Key])
		}
	}

	// Invoke reduce function for each key, and write res to output file.
	outputFile := mergeName(jobName, reduceTaskNumber)
	fmt.Println("hello:", outputFile)
	f, err := os.Create(outputFile)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(m)
	for k, v := range m {
		res := reduceF(k, v)
		fmt.Println("res is ", res, " k is ", k, " v is ", v)
		// I made a big mistake here
		// if err := enc.Encode(&res); err != nil {
		if err := enc.Encode(&KeyValue{k, res}); err != nil {
			log.Fatal(err)
		}
	}
}
