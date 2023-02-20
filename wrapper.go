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
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	DB_CONNECT_TIMEOUT_SECS    = 10
	DB_DISCONNECT_TIMEOUT_SECS = 5
	DB_GETMANY_TIMEOUT_SECS    = 30
)

type DbClient struct {
	client *mongo.Client
	DbName string
	DbUrl  string
}

func NewDbClient(dbName, dbUrl string) *DbClient {
	return &DbClient{
		DbName: dbName,
		DbUrl:  dbUrl,
	}
}

func (d *DbClient) ConnectDB() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), DB_CONNECT_TIMEOUT_SECS*time.Second)
	defer cancel()
	d.client, err = mongo.Connect(ctx, options.Client().ApplyURI(d.DbUrl))
	if err != nil {
		return err
	}
	return nil
}

func (d *DbClient) DisconnectDB() error {
	if d.client == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), DB_DISCONNECT_TIMEOUT_SECS*time.Second)
	defer cancel()

	if err := d.client.Disconnect(ctx); err != nil {
		return err
	}
	return nil
}

func (d *DbClient) GetOne(collName string, filter bson.M) (map[string]interface{}, error) {
	if d.client == nil {
		return nil, errors.New("no client available")
	}
	collection := d.client.Database(d.DbName).Collection(collName)

	var result map[string]interface{}
	err := collection.FindOne(context.TODO(), filter).Decode(&result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (d *DbClient) GetMany(collName string, filter bson.M) ([]map[string]interface{}, error) {
	if d.client == nil {
		return nil, errors.New("no client available")
	}
	collection := d.client.Database(d.DbName).Collection(collName)

	ctx, cancel := context.WithTimeout(context.Background(), DB_GETMANY_TIMEOUT_SECS*time.Second)
	defer cancel()

	cur, err := collection.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	var resultArray []map[string]interface{}
	if err := cur.All(ctx, &resultArray); err != nil {
		return nil, err
	}
	return resultArray, nil
}

// Update existing doc if found. If not found insert new one
func (d *DbClient) UpdateInsertOne(collName string, filter bson.M, putData bson.M) (int, error) {
	if d.client == nil {
		return 0, errors.New("no client available")
	}
	collection := d.client.Database(d.DbName).Collection(collName)

	// Specify the Upsert option to insert a new document if a document matching
	// the filter isn't found.
	opts := options.Update().SetUpsert(true)
	res, err := collection.UpdateOne(context.TODO(), filter, bson.M{"$set": putData}, opts)
	if err != nil {
		return 0, err
	}
	return int(res.MatchedCount), err
}

// Update only if present. Do not insert
func (d *DbClient) UpdateOne(collName string, filter bson.M, putData bson.M) (int, error) {
	if d.client == nil {
		return 0, errors.New("no client available")
	}
	collection := d.client.Database(d.DbName).Collection(collName)

	// Specify the Upsert option to insert a new document if a document matching
	// the filter isn't found.
	opts := options.Update().SetUpsert(false)
	res, err := collection.UpdateOne(context.TODO(), filter, bson.M{"$set": putData}, opts)
	if err != nil {
		return 0, err
	}
	return int(res.MatchedCount), err
}

// Document will be created if not present
func (d *DbClient) GetIncrementedOne(collName string, filter bson.M, toUpdate bson.M) (map[string]interface{}, error) {
	if d.client == nil {
		return nil, errors.New("no client available")
	}
	collection := d.client.Database(d.DbName).Collection(collName)

	// Specify the Upsert option to insert a new document if a document matching
	// the filter isn't found.
	upsert := true
	returnDoc := options.After // Return document post update
	opts := options.FindOneAndUpdateOptions{
		Upsert:         &upsert,
		ReturnDocument: &returnDoc,
	}
	res := collection.FindOneAndUpdate(context.TODO(), filter, toUpdate, &opts)
	if res.Err() != nil {
		return nil, res.Err()
	}

	var updatedDoc map[string]interface{}
	err := res.Decode(&updatedDoc)
	return updatedDoc, err
}

// InsertOne
func (d *DbClient) InsertOne(collName string, putData interface{}) error {
	if d.client == nil {
		return errors.New("no client available")
	}
	collection := d.client.Database(d.DbName).Collection(collName)
	_, err := collection.InsertOne(context.TODO(), putData)
	return err
}

// InsertMany
func (d *DbClient) InsertMany(collName string, putData []interface{}) error {
	if d.client == nil {
		return errors.New("no client available")
	}
	collection := d.client.Database(d.DbName).Collection(collName)
	ids, err := collection.InsertMany(context.TODO(), putData)
	if err == nil {
		if len(ids.InsertedIDs) != len(putData) {
			return fmt.Errorf("only %v records inserted", len(ids.InsertedIDs))
		}
	}
	return err
}

// Updates existing one if present
func (d *DbClient) UpdateMany(collName string, filterArray []bson.M, putDataArray []bson.M) error {
	if d.client == nil {
		return errors.New("no client available")
	}

	for i, putData := range putDataArray {
		filter := filterArray[i]
		matchCnt, err := d.UpdateOne(collName, filter, putData)
		if err != nil {
			return fmt.Errorf("put failed on index[%v] with error: %s", i, err.Error())
		} else if matchCnt == 0 {
			return fmt.Errorf("no docs matching filter")
		}
	}
	return nil
}

func (d *DbClient) DeleteOne(collName string, filter bson.M) (int, error) {
	if d.client == nil {
		return 0, errors.New("no client available")
	}
	collection := d.client.Database(d.DbName).Collection(collName)
	res, err := collection.DeleteOne(context.TODO(), filter)
	if err != nil {
		return 0, err
	}
	return int(res.DeletedCount), nil
}

func (d *DbClient) DeleteMany(collName string, filter bson.M) (int, error) {
	if d.client == nil {
		return 0, errors.New("no client available")
	}

	collection := d.client.Database(d.DbName).Collection(collName)
	result, err := collection.DeleteMany(context.TODO(), filter)
	if err != nil {
		return 0, err
	}
	return int(result.DeletedCount), nil
}

func (d *DbClient) DropCollection(collName string) error {
	if d.client == nil {
		return errors.New("no client available")
	}
	collection := d.client.Database(d.DbName).Collection(collName)
	return collection.Drop(context.TODO())
}

func (d *DbClient) CountRecords(collName string, filter bson.M) (int64, error) {
	if d.client == nil {
		return 0, errors.New("no client available")
	}
	collection := d.client.Database(d.DbName).Collection(collName)
	return collection.CountDocuments(context.TODO(), filter)
}
func (d *DbClient) DropDb() error {
	if d.client == nil {
		return errors.New("no client available")
	}
	db := d.client.Database(d.DbName)
	return db.Drop(context.TODO())
}
