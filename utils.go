// Copyright 2023 Spry Fox Networks
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dbapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"runtime"

	"github.com/mitchellh/mapstructure"
	"go.mongodb.org/mongo-driver/bson"
)

func errCtx(err error) error {
	pc, _, lineno, _ := runtime.Caller(1)

	errString := fmt.Sprintf("[%s:%d] ", runtime.FuncForPC(pc).Name(), lineno)
	if err != nil {
		errString = errString + err.Error()
	}
	return errors.New(errString)
}

func toBsonM(data interface{}) (bson.M, error) {
	tmp, err := json.Marshal(data)
	if err != nil {
		return nil, errCtx(err)
	}
	var putData = bson.M{}
	err = json.Unmarshal(tmp, &putData)
	if err != nil {
		return nil, errCtx(err)
	}
	return putData, nil
}

func decodeMapStructure(config *mapstructure.DecoderConfig, data interface{}) error {
	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return err
	}

	return decoder.Decode(data)
}
