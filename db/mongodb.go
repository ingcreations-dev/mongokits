package db

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"os"
	"strconv"
	"time"
)

var clients = make(map[string]*mongoClient)
type mongoClient struct {
	database *mongo.Database
	duration time.Duration
	options *MongoOptions
}

func GetConnection() *mongoClient{
	if client,exist := clients["default"]; exist {
		return client
	}else {
		return initializer()
	}
}

/**
通过配置获取数据库客户端连接
 */
func GetClientByName(mongoOptions *MongoOptions) (*mongoClient,error) {
	if client,exists := clients[mongoOptions.name]; !exists {
		database,err := createMongoDatabase(mongoOptions.server,mongoOptions.db,mongoOptions.timeout)
		if err != nil {
			return nil,err
		}

		client = &mongoClient{
			database : database,
			duration: time.Duration(mongoOptions.timeout) * time.Second,
			options: mongoOptions,
		}
		clients[mongoOptions.name] = client
		return client,nil
	} else {
		return client,nil
	}
}

/**
初始化默认配置Mongodb数据库链接
 */
func initializer() *mongoClient {
	server := os.Getenv("MONGODB_SERVER")
	db := os.Getenv("MONGODB_DB")
	userName := os.Getenv("MONGODB_USER_NAME")
	userPass := os.Getenv("MONGODB_USER_PASSWORD")
	timeout,_ := strconv.Atoi(os.Getenv("MONGODB_TIMEOUT"))
	database,err := createMongoDatabase(server,db,timeout)
	if err != nil{
		panic("Default mongodb config not found")
	}

	op := &MongoOptions{
		name:     "default",
		server:   server,
		db:       db,
		timeout:  timeout,
		userName: userName,
		userPass: userPass,
	}
	client := &mongoClient{
		database : database,
		duration: time.Duration(timeout) * time.Second,
		options: op,
	}
	clients[op.name] = client
	return client
}

func createMongoDatabase(server string,db string,timeout int) (*mongo.Database,error){
	client,err := mongo.NewClient(options.Client().ApplyURI(server))
	if err != nil {
		return nil,err
	}
	dur := time.Duration(timeout) * time.Second
	ctx,_ := context.WithTimeout(context.Background(),dur)
	client.Connect(ctx)

	err = client.Ping(ctx,readpref.Primary())
	if err != nil{
		return nil,err
	}
	return client.Database(db),nil
}

func (client *mongoClient) GetCollection(tableName string){
	collections := client.database.Collection(tableName)

	println(collections.Name())
}

func (client *mongoClient) Save(tableName string,table interface{}) error{
	ctx :=client.GetCtx()
	result,err := client.database.Collection(tableName).InsertOne(ctx,table)
	if err != nil{
		return err
	}
	fmt.Println("Inserted a single document: ", result.InsertedID)
	return nil
}

func (client *mongoClient) Update(tableName string,filter bson.M,setter bson.D) error {
	ctx :=client.GetCtx()
	_,err := client.database.Collection(tableName).UpdateOne(ctx,filter,setter)
	return err
}
func (client *mongoClient) FindOneAndReplace(tableName string,filter bson.M,document interface{}) error {
	ctx :=client.GetCtx()
	return client.database.Collection(tableName).FindOneAndReplace(ctx,filter,document).Err()
}

func (client *mongoClient) UpdateMany(tableName string,filter bson.M,setter interface{}) error {
	ctx :=client.GetCtx()
	_,err := client.database.Collection(tableName).UpdateMany(ctx,filter,setter)
	return err
}

/**
通过条件查询一个文档
 */
func (client *mongoClient) FindOne(tableName string,filter bson.M,table interface{}) error{
	result := client.database.Collection(tableName).FindOne(client.GetCtx(),filter)
	if result.Err() != nil{
		return result.Err()
	}
	err := result.Decode(table)
	if err != nil{
		return err
	}

	return nil
}

func (client *mongoClient) FindCount(tableName string,filter bson.M) (int64,error){
	return client.database.Collection(tableName).CountDocuments(client.GetCtx(),filter)
}

func (client *mongoClient) Delete(tableName string,filter bson.M) error{
	_,err := client.database.Collection(tableName).DeleteOne(client.GetCtx(),filter)
	return err
}

/**
通过条件查询列表
 */
func (client *mongoClient) FindAllByCondition(tableName string,filter bson.M) (*mongo.Cursor,error) {
	return client.database.Collection(tableName).Find(client.GetCtx(),filter)
}

func (client *mongoClient) FindAll(tableName string)(*mongo.Cursor,error){
	return client.database.Collection(tableName).Find(client.GetCtx(),nil)
}

func (client *mongoClient) FindAndFill(tableName string,filter bson.M,array interface{}) error {

	cursor,err := client.database.Collection(tableName).Find(client.GetCtx(),filter)
	if err != nil{
		return err
	}
	ctx := client.GetCtx()
	defer cursor.Close(ctx)
	cursor.All(ctx,array)
	return nil
}

func (client *mongoClient) GetCtx() context.Context{
	ctx,_ := context.WithTimeout(context.Background(),client.duration)
	return ctx
}

func (client *mongoClient) GetDuration() time.Duration{
	return client.duration
}
