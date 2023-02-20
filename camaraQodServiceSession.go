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
	"github.com/mitchellh/mapstructure"
	"github.com/sfnuser/camara/qodmodels/db"
	"go.mongodb.org/mongo-driver/bson"
)

func decodeServiceUeFlow(data interface{}) (*db.ServiceQoDUeFlow, error) {
	var serviceData db.ServiceQoDUeFlow
	config := mapstructure.DecoderConfig{
		Result: &serviceData,
	}
	err := decodeMapStructure(&config, data)
	if err != nil {
		return nil, errCtx(err)
	}
	return &serviceData, nil
}
func decodeServiceUeSession(data interface{}) (*db.ServiceQoDUeSession, error) {
	var serviceData db.ServiceQoDUeSession
	config := mapstructure.DecoderConfig{
		Result: &serviceData,
	}
	err := decodeMapStructure(&config, data)
	if err != nil {
		return nil, errCtx(err)
	}
	return &serviceData, nil
}
func (d *DbApi) GetCamaraQoDServiceUeFlow(ueIpv4Addr, scsAsId string) (*db.ServiceQoDUeFlow, error) {
	filter := bson.M{
		"ueIpv4Addr": ueIpv4Addr,
		"scsAsId":    scsAsId,
	}
	getData, err := d.cli.GetOne(COLLECTION_CAMARA_QOD_SERVICE_UE_FLOW, filter)
	if err != nil {
		return nil, errCtx(err)
	}
	serviceData, err := decodeServiceUeFlow(getData)
	if err != nil {
		return nil, errCtx(err)
	}
	return serviceData, nil
}
func (d *DbApi) PutCamaraQoDServiceUeFlow(ueIpv4Addr, scsAsId string, data *db.ServiceQoDUeFlow) (int, error) {
	filter := bson.M{
		"ueIpv4Addr": ueIpv4Addr,
		"scsAsId":    scsAsId,
	}
	putData, err := toBsonM(data)
	if err != nil {
		return 0, errCtx(err)
	}
	return d.cli.UpdateInsertOne(COLLECTION_CAMARA_QOD_SERVICE_UE_FLOW, filter, putData)
}
func (d *DbApi) GetCamaraQoDServiceIncrementUeFlow(ueIpv4Addr, scsAsId string) (*db.ServiceQoDUeFlow, error) {
	filter := bson.M{
		"ueIpv4Addr": ueIpv4Addr,
		"scsAsId":    scsAsId,
	}
	update := bson.M{
		"$inc": bson.M{
			"FlowCounter": 1,
		},
	}
	getData, err := d.cli.GetIncrementedOne(COLLECTION_CAMARA_QOD_SERVICE_UE_FLOW, filter, update)
	if err != nil {
		return nil, errCtx(err)
	}
	serviceData, err := decodeServiceUeFlow(getData)
	if err != nil {
		return nil, errCtx(err)
	}
	return serviceData, nil
}
func (d *DbApi) GetCamaraQoDServiceUeSession(sessionId string) (*db.ServiceQoDUeSession, error) {
	filter := bson.M{
		"sessionId": sessionId,
	}
	getData, err := d.cli.GetOne(COLLECTION_CAMARA_QOD_SERVICE_SESSION, filter)
	if err != nil {
		return nil, errCtx(err)
	}
	serviceData, err := decodeServiceUeSession(getData)
	if err != nil {
		return nil, errCtx(err)
	}
	return serviceData, nil
}
func (d *DbApi) PutCamaraQoDServiceUeSession(ueIpv4Addr, sessionId string, data *db.ServiceQoDUeSession) (int, error) {
	filter := bson.M{
		"ueIpv4Addr": ueIpv4Addr,
		"sessionId":  sessionId,
	}
	putData, err := toBsonM(data)
	if err != nil {
		return 0, errCtx(err)
	}
	return d.cli.UpdateInsertOne(COLLECTION_CAMARA_QOD_SERVICE_SESSION, filter, putData)
}
func (d *DbApi) DeleteCamaraQoDServiceUeSession(sessionId string) (int, error) {
	filter := bson.M{
		"sessionId": sessionId,
	}
	return d.cli.DeleteOne(COLLECTION_CAMARA_QOD_SERVICE_SESSION, filter)
}
func (d *DbApi) GetNumCamaraQoDSericeUeSessions(ueIpv4Addr, scsAsId string) (int, error) {
	filter := bson.M{
		"ueIpv4Addr": ueIpv4Addr,
		"scsAsId":    scsAsId,
	}
	count, err := d.cli.CountRecords(COLLECTION_CAMARA_QOD_SERVICE_SESSION, filter)
	if err != nil {
		return 0, errCtx(err)
	}
	return int(count), nil
}
func (d *DbApi) GetAllCamaraQoDServiceUeSession(ueIpv4Addr, scsAsId, qosProfile string) (*[]db.ServiceQoDUeSession, error) {
	filter := bson.M{
		"ueIpv4Addr":     ueIpv4Addr,
		"scsAsId":        scsAsId,
		"sessionReq.Qos": qosProfile,
	}
	getData, err := d.cli.GetMany(COLLECTION_CAMARA_QOD_SERVICE_SESSION, filter)
	if err != nil {
		return nil, errCtx(err)
	} else if len(getData) == 0 {
		return nil, nil
	}
	var ueSessions []db.ServiceQoDUeSession
	config := mapstructure.DecoderConfig{
		Result: &ueSessions,
	}
	err = decodeMapStructure(&config, getData)
	if err != nil {
		return nil, errCtx(err)
	}
	return &ueSessions, nil
}
func (d *DbApi) GetAllCamaraQoDServiceUeFlows() (*[]db.ServiceQoDUeFlow, error) {
	filter := bson.M{}
	getData, err := d.cli.GetMany(COLLECTION_CAMARA_QOD_SERVICE_UE_FLOW, filter)
	if err != nil {
		return nil, errCtx(err)
	} else if len(getData) == 0 {
		return nil, nil
	}
	var ueFlows []db.ServiceQoDUeFlow
	config := mapstructure.DecoderConfig{
		Result: &ueFlows,
	}
	err = decodeMapStructure(&config, getData)
	if err != nil {
		return nil, errCtx(err)
	}
	return &ueFlows, nil
}
