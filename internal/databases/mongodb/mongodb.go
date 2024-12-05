package mongodb

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/go-github/github"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func ConnectByString(connection_string string, ctx context.Context) (*mongo.Client, error) {

	clientOptions := options.Client().ApplyURI(connection_string)
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		// log.Printf("MongoDB Connect: Client Error: %s", err)
		return nil, err
	}

	// Ping the primary to verify connection establishment
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	err = client.Ping(ctx, nil)
	if err != nil {
		// log.Printf("MongoDB Connect: Ping Error: %s", err)
		return nil, err
	}

	return client, nil

}

func InitProfileOptions(connectionString string, database string) error {

	ctx := context.Background()
	mongo, err := ConnectByString(connectionString, ctx)
	if err != nil {
		log.Printf("Error: MongoDB: Connect: %v", err)
		return err
	}
	defer mongo.Disconnect(ctx)

	db := mongo.Database(database)

	profileCommand := bson.D{
		{Key: "profile", Value: 2},
		{Key: "slowms", Value: 200},
		{Key: "ratelimit", Value: 100},
	}

	var result bson.M
	err = db.RunCommand(ctx, profileCommand).Decode(&result)
	if err != nil {
		log.Printf("Error: MongoDB: RunCommand: %v", err)
		return err
	} else {
		log.Printf("Profile options for PMM set successfully: %v", result)
	}

	return nil
}

// CheckMongoDB checks the connection to the MongoDB server using the provided connection string.
// It returns "Connected" if the connection is successful, otherwise it returns the error message.
//
// Arguments:
//   - connectionString: string containing the MongoDB connection string.
//
// Returns:
//   - string: "Connected" if successful, otherwise the error message.
func CheckMongoDB(connectionString string) string {
	connectCtx, connectCancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	defer connectCancel()

	// Use the ConnectByString function to connect to MongoDB
	client, err := ConnectByString(connectionString, connectCtx)
	if err != nil {
		return fmt.Sprintf("Error connecting to MongoDB: %v", err)
	}
	// Disconnect immediately after connecting
	defer client.Disconnect(context.Background())

	return "Connected"
}

func DeleteSchema(connectionString string, dbName string) error {
	ctx := context.Background()
	mongo, err := ConnectByString(connectionString, ctx)
	if err != nil {
		log.Printf("Error: MongoDB: Connect: %v", err)
		return err
	}
	defer mongo.Disconnect(ctx)

	dbList, err := mongo.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		log.Printf("Error listing databases: %v", err)
		return err
	}

	dbExists := false
	for _, db := range dbList {
		if db == dbName {
			dbExists = true
			break
		}
	}

	if dbExists {
		log.Printf("Database %s exists, deleting database", dbName)
		err = mongo.Database(dbName).Drop(ctx)
		if err != nil {
			log.Printf("Error deleting database %s: %v", dbName, err)
			return err
		}
		log.Printf("Database %s deleted successfully", dbName)
	} else {
		log.Printf("Database %s does not exist", dbName)
	}

	return nil
}

// GetPullsLatestUpdates retrieves the latest update times for each repository from a MongoDB database.
// It connects to the MongoDB database using the provided connection string and queries the update times.
//
// Arguments:
//   - dbConfig: map[string]string containing the database configuration,
//     including the connection string under the key "connectionString".
//
// Returns:
//   - map[string]string: A map where keys are repository names and values are the latest update times.
//   - error: An error object if an error occurs, otherwise nil.
func GetPullsLatestUpdates(dbConfig map[string]string) (map[string]string, error) {
	ctx := context.Background()

	// Connect to the MongoDB database.
	client, err := ConnectByString(dbConfig["connectionString"], ctx)
	if err != nil {
		log.Printf("MongoDB: Connect Error: message: %s", err)
		return nil, err
	}
	defer client.Disconnect(ctx)

	db := client.Database(dbConfig["database"])
	collection := db.Collection("pulls")

	// Define the aggregation pipeline to get the latest update times.
	pipeline := mongo.Pipeline{
		{{Key: "$sort", Value: bson.D{{Key: "updatedat", Value: -1}}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$repo"},
			{Key: "updatedat", Value: bson.D{{Key: "$first", Value: "$updatedat"}}},
		}}},
	}

	cursor, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	lastUpdates := make(map[string]string)
	for cursor.Next(ctx) {
		var result struct {
			Repo      string    `bson:"_id"`
			UpdatedAt time.Time `bson:"updatedat"`
		}
		if err := cursor.Decode(&result); err != nil {
			return nil, err
		}
		lastUpdates[result.Repo] = result.UpdatedAt.UTC().Format(time.RFC3339)
	}

	if err := cursor.Err(); err != nil {
		return nil, err
	}

	return lastUpdates, nil
}

// GetDatasetLatestUpdates retrieves the latest update times for the dataset from the reports_dataset collection.
func GetDatasetLatestUpdates(dbConfig map[string]string) (string, error) {
	ctx := context.Background()

	// Connect to the MongoDB database.
	client, err := ConnectByString(dbConfig["connectionString"], ctx)
	if err != nil {
		log.Printf("MongoDB: Connect Error: %s", err)
		return "", err
	}
	defer client.Disconnect(ctx)

	db := client.Database(dbConfig["database"])
	collection := db.Collection("reports_dataset")

	// Define the aggregation pipeline to get the latest update time.
	pipeline := mongo.Pipeline{
		{{Key: "$sort", Value: bson.D{{Key: "finishedat", Value: -1}}}},
		{{Key: "$limit", Value: 1}},
	}

	cursor, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		return "", err
	}
	defer cursor.Close(ctx)

	var lastUpdate string
	if cursor.Next(ctx) {
		var result struct {
			FinishedAt string `bson:"finishedat"`
		}
		if err := cursor.Decode(&result); err != nil {
			return "", err
		}
		lastUpdate = result.FinishedAt
	}

	if err := cursor.Err(); err != nil {
		return "", err
	}

	return lastUpdate, nil
}

// CreateIndex creates an index in the specified collection using the provided keys and options.
// It ensures that the combination of specified keys is unique.
//
// Arguments:
//   - client: *mongo.Client to connect to the MongoDB server.
//   - dbName: string containing the name of the database.
//   - collectionName: string containing the name of the collection.
//   - indexKeys: bson.D containing the keys for the index.
//   - unique: bool indicating whether the index should be unique.
//
// Returns:
//   - error: An error object if an error occurs, otherwise nil.
func CreateIndex(client *mongo.Client, dbName string, collectionName string, indexKeys bson.D, unique bool) error {
	ctx := context.Background()

	collection := client.Database(dbName).Collection(collectionName)

	indexModel := mongo.IndexModel{
		Keys:    indexKeys,
		Options: options.Index().SetUnique(unique),
	}

	_, err := collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return err
	}
	return nil
}

func GetUniqueIntegers(client *mongo.Client, dbName, collectionName string, key string) ([]int64, error) {
	ctx := context.Background()
	collection := client.Database(dbName).Collection(collectionName)

	cmd := bson.D{
		{Key: "distinct", Value: collectionName},
		{Key: "key", Value: key},
	}

	var result struct {
		Values []int64 `bson:"values"`
	}
	err := collection.Database().RunCommand(ctx, cmd).Decode(&result)
	if err != nil {
		return nil, err
	}

	return result.Values, nil
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

func FindPullRequests(client *mongo.Client, dbName string, collectionName string, filter bson.D, sort bson.D, limit int64) ([]*github.PullRequest, error) {

	ctx := context.Background()
	collection := client.Database(dbName).Collection(collectionName)

	var pullRequests []*github.PullRequest
	var opts *options.FindOptions

	if limit > 0 {
		opts = options.Find().SetSort(sort).SetLimit(limit)
	} else {
		opts = options.Find().SetSort(sort)
	}

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

func FindDocuments(client *mongo.Client, dbName string, collectionName string, filter bson.D, sort bson.D, limit int64) ([]interface{}, error) {

	ctx := context.Background()
	collection := client.Database(dbName).Collection(collectionName)

	var documents []interface{}
	var opts *options.FindOptions

	if limit > 0 {
		opts = options.Find().SetSort(sort).SetLimit(limit)
	} else {
		opts = options.Find().SetSort(sort)
	}

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

func FindRepos(client *mongo.Client, dbName string, collectionName string, filter bson.D, sort bson.D, limit int64) ([]*github.Repository, error) {

	ctx := context.Background()
	collection := client.Database(dbName).Collection(collectionName)

	var Repos []*github.Repository
	var opts *options.FindOptions

	if limit > 0 {
		opts = options.Find().SetSort(sort).SetLimit(limit)
	} else {
		opts = options.Find().SetSort(sort)
	}

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

func GetNestedField(data map[string]interface{}, fieldPath string) (interface{}, error) {
	fieldParts := strings.Split(fieldPath, ".")
	var value interface{} = data

	for _, part := range fieldParts {
		if v, ok := value.(map[string]interface{})[part]; ok {
			value = v
		} else {
			return nil, fmt.Errorf("field %s not found", fieldPath)
		}
	}
	return value, nil
}
