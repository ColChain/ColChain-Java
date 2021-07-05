package org.colchain.colchain.util;

import com.google.gson.*;
import org.colchain.colchain.knowledgechain.impl.ChainEntry;
import org.colchain.colchain.transaction.ITransaction;
import org.colchain.colchain.transaction.Operation;
import org.colchain.colchain.transaction.impl.TransactionFactory;
import org.colchain.index.util.Triple;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class TransactionSerializer implements JsonSerializer<ITransaction>, JsonDeserializer<ITransaction> {
    @Override
    public ITransaction deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        return deserializeTransaction(jsonElement);
    }

    @Override
    public JsonElement serialize(ITransaction transaction, Type type, JsonSerializationContext jsonSerializationContext) {
        return serializeTransaction(transaction);
    }

    private JsonElement serializeTransaction(ITransaction transaction) {
        JsonObject obj = new JsonObject();

        obj.addProperty("fragmentId", transaction.getFragmentId());
        obj.addProperty("author", transaction.getAuthor());
        obj.addProperty("id", transaction.getId());
        obj.addProperty("timestamp", transaction.getTimestamp());

        JsonArray arr = new JsonArray();
        List<Operation> ops = transaction.getOperations();
        for(Operation op : ops) {
            arr.add(serializeOperation(op));
        }
        obj.add("operations", arr);

        return obj;
    }

    private ITransaction deserializeTransaction(JsonElement element) {
        JsonObject obj = element.getAsJsonObject();
        String fid = obj.get("fragmentId").getAsString();
        String author = obj.get("author").getAsString();
        String id = obj.get("id").getAsString();
        long timestamp = obj.get("timestamp").getAsLong();

        List<Operation> lst = new ArrayList<>();
        JsonArray arr = obj.get("operations").getAsJsonArray();
        for(int i = 0; i < arr.size(); i++) {
            lst.add(deserializeOperation(arr.get(i)));
        }

        return TransactionFactory.getTransaction(lst, fid, author, id, timestamp);
    }

    private JsonElement serializeOperation(Operation operation) {
        JsonObject obj = new JsonObject();

        obj.addProperty("type", operation.getType().toString());
        obj.addProperty("s", operation.getTriple().getSubject());
        obj.addProperty("p", operation.getTriple().getPredicate());
        obj.addProperty("o", operation.getTriple().getObject());

        return obj;
    }

    private Operation deserializeOperation(JsonElement element) {
        JsonObject obj = element.getAsJsonObject();
        JsonObject tObj = obj.get("triple").getAsJsonObject();
        return new Operation(Operation.OperationType.valueOf(obj.get("type").getAsString()),
                new Triple(tObj.get("s").getAsString(), tObj.get("p").getAsString(), tObj.get("o").getAsString()));
    }
}
