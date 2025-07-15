package mr

import (
	"io/ioutil"
	"os"
	"fmt"
	"encoding/json"
)

func GetFileContent(filename string) (string, error){
	/*
	Return content of a file as string
	*/
	file, err := os.Open(filename)
	if err != nil {
		return "", fmt.Errorf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return "", fmt.Errorf("cannot read %v", filename)
	}
	defer file.Close()

	return string(content), nil
}

func AppendKvToFile(fileName string, kv KeyValue) error{
	/*
	Append key-value pair to a file
	*/
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	err = enc.Encode(&kv)
	
	return err
}


func FileExists(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || !os.IsNotExist(err)
}

func GetKvFromFile(fileName string) ([]KeyValue, error){
	/*
	Get pairs of key-value from file and store them in a slice
	*/
	file, err := os.Open(fileName)
	if err != nil {
		return []KeyValue{}, err
	}
	defer file.Close()

	dec := json.NewDecoder(file)
		
	kva := []KeyValue{}
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	
	return kva, nil
}