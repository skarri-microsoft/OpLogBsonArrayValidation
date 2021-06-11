using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading.Tasks;

using MongoDB.Bson;
using MongoDB.Driver;

namespace OpLogBsonArrayValidation
{
    internal class Program
    {
        private static string collectionName = ConfigurationManager.AppSettings["collectionName"];
        private static IMongoDatabase database;
        private static string dbName = ConfigurationManager.AppSettings["dbName"];
        private static long docsCount = 0;
        private static IMongoCollection<BsonDocument> docStoreCollection;
        private static MongoClient mongoClient;

        private static async Task InsertSet1()
        {
            var tasks = new List<Task>();
            for (int j = 0; j < 25; j++)
            {
                tasks.Add(ReplaceSample1(3));
                tasks.Add(UpdateSet1(3));
                tasks.Add(UpdateUnset1(3));
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);
            Console.WriteLine("Insert set 1 completed");
        }

        private static async Task InsertSet2()
        {
            var tasks = new List<Task>();
            for (int j = 0; j < 25; j++)
            {
                tasks.Add(ReplaceSample2(4));
                tasks.Add(UpdateSet2(4));
                tasks.Add(UpdateSet2_1(4));
                tasks.Add(UpdateUnset2(4));
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);
            Console.WriteLine("Insert set 2 completed");
        }

        private static async Task InsertSet3()
        {
            var tasks = new List<Task>();
            for (int j = 0; j < 25; j++)
            {
                tasks.Add(UpdateSet3(5));
                tasks.Add(UpdateSet3_1(5));
                tasks.Add(UpdateUnset3(5));
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);
            Console.WriteLine("Insert set 3 completed");
        }

        private static void Main(string[] args)
        {
            string connectionString =
              ConfigurationManager.AppSettings["conn"];
            MongoClientSettings settings = MongoClientSettings.FromUrl(
                new MongoUrl(connectionString)
            );
            mongoClient = new MongoClient(settings);
            database = mongoClient.GetDatabase(dbName);
            docStoreCollection = database.GetCollection<BsonDocument>(collectionName);
            SetSampleDocs();

            while (true)
            {
                var tasks = new List<Task>();
                tasks.Add(InsertSet1());
                tasks.Add(InsertSet2());
                tasks.Add(InsertSet3());
                System.Threading.Thread.Sleep(5000);
            }
        }

        private static async Task ReplaceSample1(int id)
        {
            string _idFilter = "{ \"_id\" : \"" + id.ToString() + "\" }";
            string replacement = "{ \"p\" : [{\"val\": \"Replace1\"},{\"val\": \"Replace2\" }]}";
            await docStoreCollection.ReplaceOneAsync(BsonDocument.Parse(_idFilter), BsonDocument.Parse(replacement), new ReplaceOptions() { IsUpsert = true });
        }

        private static async Task ReplaceSample2(int id)
        {
            string _idFilter = "{ \"_id\" : \"" + id.ToString() + "\" }";
            string replacement = "{ \"parent\" : { \"child\": [ { \"val\": \"NestedReplaceSet1\" }, { \"val\": \"NestedReplaceSet2\" } ] }}";
            await docStoreCollection.ReplaceOneAsync(BsonDocument.Parse(_idFilter), BsonDocument.Parse(replacement), new ReplaceOptions() { IsUpsert = true });
        }

        private static void SetSampleDocs()
        {
            for (int i = 1; i <= 5; i++)
            {
                try
                {
                    docStoreCollection.InsertOne((BsonDocument.Parse("{ \"_id\" : \"" + i.ToString() + "\" }")));
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }

        private static async Task UpdateSet1(int id)
        {
            string _idFilter = "{ \"_id\" : \"" + id.ToString() + "\" }";
            string setDoc = "{ $set:{\"p.0.val\" : \"IndexSet\" }}";
            await docStoreCollection.UpdateOneAsync(BsonDocument.Parse(_idFilter), BsonDocument.Parse(setDoc));
        }

        private static async Task UpdateSet2(int id)
        {
            string _idFilter = "{ \"_id\" : \"" + id.ToString() + "\" }";
            string setDoc = "{ $set:{\"parent.child.0.val\" : \"NestedIndexSet\" }}";
            await docStoreCollection.UpdateOneAsync(BsonDocument.Parse(_idFilter), BsonDocument.Parse(setDoc));
        }

        private static async Task UpdateSet2_1(int id)
        {
            string _idFilter = "{ \"_id\" : \"" + id.ToString() + "\" }";
            string setDoc = "{ $set:{\"p\" : [ { \"val\" : \"Set1\" }, { \"val\" : \"Set2\" } ] }}";
            await docStoreCollection.UpdateOneAsync(BsonDocument.Parse(_idFilter), BsonDocument.Parse(setDoc));
        }

        private static async Task UpdateSet3(int id)
        {
            string _idFilter = "{ \"_id\" : \"" + id.ToString() + "\" }";
            string setDoc = "{ $set:{\"parent.child\" : [ { \"gchilds1\" : [{\"name\":\"ArrayWithInArrayElementSet1\"},{\"name\":\"ArrayWithInArrayElementSet2\"}] }, { \"gchilds2\" : [{\"name\":\"ArrayWithInArrayElementSet3\"},{\"name\":\"ArrayWithInArrayElementSet4\"}] } ]}}";
            await docStoreCollection.UpdateOneAsync(BsonDocument.Parse(_idFilter), BsonDocument.Parse(setDoc));
        }

        private static async Task UpdateSet3_1(int id)
        {
            string _idFilter = "{ \"_id\" : \"" + id.ToString() + "\" }";
            string setDoc = "{ \"$set\" : { \"parent.child.0.0.name\" : \"ArrayWithInArrayElementSet2-updated\" }}";
            await docStoreCollection.UpdateOneAsync(BsonDocument.Parse(_idFilter), BsonDocument.Parse(setDoc));
        }

        private static async Task UpdateUnset1(int id)
        {
            string _idFilter = "{ \"_id\" : \"" + id.ToString() + "\" }";
            string unSetDoc = "{ $unset:{\"p.0.val\" : true }}";
            await docStoreCollection.UpdateOneAsync(BsonDocument.Parse(_idFilter), BsonDocument.Parse(unSetDoc));
        }

        private static async Task UpdateUnset2(int id)
        {
            string _idFilter = "{ \"_id\" : \"" + id.ToString() + "\" }";
            string unSetDoc = @"{ $unset:{""parent.child.0.val"" : true }}";
            await docStoreCollection.UpdateOneAsync(BsonDocument.Parse(_idFilter), BsonDocument.Parse(unSetDoc));
        }

        private static async Task UpdateUnset3(int id)
        {
            string _idFilter = "{ \"_id\" : \"" + id.ToString() + "\" }";
            string unSetDoc = "{ \"$unset\" : { \"parent.child.0.0.name\" : true }}";
            await docStoreCollection.UpdateOneAsync(BsonDocument.Parse(_idFilter), BsonDocument.Parse(unSetDoc));
        }
    }
}