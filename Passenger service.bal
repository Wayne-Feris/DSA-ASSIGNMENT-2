import ballerina/http;
import ballerina/uuid;
import ballerinax/mongodb;

type Passenger record {|
    string id;
    string name;
    string email;
    string password;
    decimal balance;
|};

type RegisterRequest record {|
    string name;
    string email;
    string password;
|};

type LoginRequest record {|
    string email;
    string password;
|};

configurable string mongoUrl = "mongodb://admin:password@localhost:27017";

mongodb:Client mongoClient = check new ({
    connection: {
        url: mongoUrl
    }
});

service /passenger on new http:Listener(8081) {
     resource function post register(@http:Payload RegisterRequest req) returns http:Created|http:Conflict|error {
        mongodb:Database db = check mongoClient->getDatabase("transport_db");
        mongodb:Collection passengers = check db->getCollection("passengers");
         map<json> filter = {"email": req.email};
        stream<Passenger, error?> existingStream = check passengers->find(filter);
        Passenger[]|error existing = from Passenger p in existingStream select p;
        if existing is Passenger[] && existing.length() > 0 {
            return http:CONFLICT;
        }
        Passenger passenger = {
            id: uuid:createType1AsString(),
            name: req.name,
            email: req.email,
            password: req.password, 
            balance: 0.0
        };
        check passengers->insertOne(passenger);
        return http:CREATED;
    }
     resource function post login(@http:Payload LoginRequest req) returns json|http:Unauthorized|error {
        mongodb:Database db = check mongoClient->getDatabase("transport_db");
        mongodb:Collection passengers = check db->getCollection("passengers");
        
        map<json> filter = {"email": req.email, "password": req.password};
        stream<Passenger, error?> passengerStream = check passengers->find(filter);
        Passenger[]|error result = from Passenger p in passengerStream select p;
        
        if result is Passenger[] && result.length() > 0 {
            Passenger p = result[0];
            return {
                "id": p.id,
                "name": p.name,
                "email": p.email,
                "balance": p.balance
            };
        }
        return http:UNAUTHORIZED;
    }
     resource function get [string passengerId]() returns json|http:NotFound|error {
        mongodb:Database db = check mongoClient->getDatabase("transport_db");
        mongodb:Collection passengers = check db->getCollection("passengers");
         map<json> filter = {"id": passengerId};
        stream<Passenger, error?> passengerStream = check passengers->find(filter);
        Passenger[]|error result = from Passenger p in passengerStream select p;
         if result is Passenger[] && result.length() > 0 {
            Passenger p = result[0];
            return {
                "id": p.id,
                "name": p.name,
                "email": p.email,
                "balance": p.balance
            };
        }
        return http:NOT_FOUND;
    }
     resource function post [string passengerId]/topup(decimal amount) returns json|http:NotFound|error {
        mongodb:Database db = check mongoClient->getDatabase("transport_db");
        mongodb:Collection passengers = check db->getCollection("passengers");
        map<json> filter = {"id": passengerId};
        map<json> update = {"$inc": {"balance": amount}};
        mongodb:UpdateResult updateResult = check passengers->updateOne(filter, update);
        if updateResult.modifiedCount == 0 {
            return http:NOT_FOUND;
        }
        return {"message": "Balance updated successfully", "amount": amount};
    }
}
