package mongodb

import (
	"context"
	"fmt"
	"log"

	app "github-stat/internal"

	"github.com/google/go-github/github"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func Connect(envVars app.EnvVars, ctx context.Context) (*mongo.Client, error) {

	ApplyURI := fmt.Sprintf("mongodb://%s:%s@%s:%s/", envVars.MongoDB.User, envVars.MongoDB.Password, envVars.MongoDB.Host, envVars.MongoDB.Port)

	log.Printf("Databases: MongoDB: Start: Connect to: %s", envVars.MongoDB.Host)

	clientOptions := options.Client().ApplyURI(ApplyURI)
	mongodb, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Printf("MySQL Error: %s", err)
		return nil, err
	}

	return mongodb, nil
}

func CountDocuments(client *mongo.Client, dbName, collectionName string, filter bson.D) (int64, error) {
	ctx := context.Background()
	collection := client.Database(dbName).Collection(collectionName)

	result, err := collection.CountDocuments(ctx, filter)
	if err != nil {
		return 0, err
	}
	return result, nil

}

func FindOnePullRequest(client *mongo.Client, dbName string, collectionName string, filter bson.D, sort bson.D) (*github.PullRequest, error) {

	ctx := context.Background()

	collection := client.Database(dbName).Collection(collectionName)

	var pullRequest github.PullRequest

	opts := options.FindOne().SetSort(sort)

	err := collection.FindOne(ctx, filter, opts).Decode(&pullRequest)
	if err != nil {
		return nil, err
	}
	return &pullRequest, nil
}

func FindOne(client *mongo.Client, dbName string, collectionName string, filter bson.D, sort bson.D) (map[string]interface{}, error) {

	ctx := context.Background()

	collection := client.Database(dbName).Collection(collectionName)

	var result map[string]interface{}

	opts := options.FindOne().SetSort(sort)

	err := collection.FindOne(ctx, filter, opts).Decode(&result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func SelectRandomDocument(client *mongo.Client, dbName, collectionName string) (map[string]interface{}, error) {
	ctx := context.Background()
	collection := client.Database(dbName).Collection(collectionName)

	pipeline := mongo.Pipeline{
		{{Key: "$sample", Value: bson.D{{Key: "size", Value: 1}}}},
	}

	cursor, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var result map[string]interface{}
	if cursor.Next(ctx) {
		if err := cursor.Decode(&result); err != nil {
			return nil, err
		}
		return result, nil
	}

	return nil, mongo.ErrNoDocuments
}

func FindPullRequests(client *mongo.Client, dbName string, collectionName string, filter bson.D, sort bson.D) ([]*github.PullRequest, error) {

	ctx := context.Background()
	collection := client.Database(dbName).Collection(collectionName)

	var pullRequests []*github.PullRequest

	opts := options.Find().SetSort(sort)

	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	if err = cursor.All(ctx, &pullRequests); err != nil {
		return nil, err
	}

	return pullRequests, nil

}

func FindDocuments(client *mongo.Client, dbName string, collectionName string, filter bson.D, sort bson.D) ([]interface{}, error) {

	ctx := context.Background()
	collection := client.Database(dbName).Collection(collectionName)

	var documents []interface{}

	opts := options.Find().SetSort(sort)

	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	if err = cursor.All(ctx, &documents); err != nil {
		return nil, err
	}

	return documents, nil

}

func FindRepos(client *mongo.Client, dbName string, collectionName string, filter bson.D, sort bson.D) ([]*github.Repository, error) {

	ctx := context.Background()
	collection := client.Database(dbName).Collection(collectionName)

	var Repos []*github.Repository

	opts := options.Find().SetSort(sort)

	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	if err = cursor.All(ctx, &Repos); err != nil {
		return nil, err
	}

	return Repos, nil

}

func InsertOneDoc(client *mongo.Client, dbName string, collectionName string, doc map[string]interface{}) (*mongo.InsertOneResult, error) {

	coll := client.Database(dbName).Collection(collectionName)

	result, err := coll.InsertOne(context.TODO(), doc)

	if err != nil {
		return nil, err
	}

	return result, nil

}

func InsertManyDocuments(client *mongo.Client, dbName string, collectionName string, docs []interface{}) (*mongo.InsertManyResult, error) {

	coll := client.Database(dbName).Collection(collectionName)

	result, err := coll.InsertMany(context.TODO(), docs)

	if err != nil {
		return nil, err
	}

	return result, nil

}

func UpsertOneDoc(client *mongo.Client, dbName string, collectionName string, doc map[string]interface{}) (*mongo.UpdateResult, error) {
	coll := client.Database(dbName).Collection(collectionName)

	filter := bson.M{"_id": doc["_id"]}
	update := bson.M{"$set": doc}
	opts := options.Update().SetUpsert(true)

	result, err := coll.UpdateOne(context.TODO(), filter, update, opts)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func UpsertManyDocuments(client *mongo.Client, dbName string, collectionName string, docs []interface{}) (*mongo.BulkWriteResult, error) {
	coll := client.Database(dbName).Collection(collectionName)

	var models []mongo.WriteModel
	for _, doc := range docs {
		filter := bson.M{"_id": doc.(bson.M)["_id"]}
		update := bson.M{"$set": doc}
		model := mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true)
		models = append(models, model)
	}

	opts := options.BulkWrite().SetOrdered(false)
	result, err := coll.BulkWrite(context.TODO(), models, opts)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func DropCollection(client *mongo.Client, dbName string, collectionName string) error {
	coll := client.Database(dbName).Collection(collectionName)
	err := coll.Drop(context.TODO())
	if err != nil {
		return err
	}
	return nil
}

func DeleteDocuments(client *mongo.Client, dbName string, collectionName string, filter bson.D) error {
	coll := client.Database(dbName).Collection(collectionName)

	_, err := coll.DeleteMany(context.TODO(), filter)
	if err != nil {
		return err
	}

	return nil
}
