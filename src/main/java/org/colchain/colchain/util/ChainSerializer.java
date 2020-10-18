package org.colchain.colchain.util;

import com.google.gson.*;
import org.colchain.index.util.Triple;
import org.colchain.colchain.knowledgechain.impl.ChainEntry;
import org.colchain.colchain.transaction.ITransaction;
import org.colchain.colchain.transaction.Operation;
import org.colchain.colchain.transaction.impl.TransactionFactory;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class ChainSerializer implements JsonSerializer<ChainEntry>, JsonDeserializer<ChainEntry> {
    @Override
    public ChainEntry deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        return deserializeChainEntry(jsonElement);
    }

    @Override
    public JsonElement serialize(ChainEntry chainEntry, Type type, JsonSerializationContext jsonSerializationContext) {
        return serializeChainEntry(chainEntry);
    }

    private JsonElement serializeChainEntry(ChainEntry entry) {
        JsonObject obj = new JsonObject();

        if(entry.isFirst()) {
            obj.addProperty("transaction", "null");
            obj.addProperty("prev", "null");
            return obj;
        }

        obj.add("transaction", serializeTransaction(entry.getTransaction()));
        obj.add("prev", serializeChainEntry(entry.previous()));

        return obj;
    }

    private ChainEntry deserializeChainEntry(JsonElement element) {
        JsonObject obj = element.getAsJsonObject();
        if(!obj.has("transaction") || !obj.has("prev"))
            return ChainEntry.getInitialEntry();

        JsonObject transObj = obj.getAsJsonObject("transaction");
        JsonObject prevObj = obj.getAsJsonObject("prev");

        if((!transObj.isJsonObject() && transObj.getAsString().equals("null")) &&
                (!prevObj.isJsonObject() && prevObj.getAsString().equals("null")))
            return ChainEntry.getInitialEntry();

        return new ChainEntry(deserializeTransaction(transObj), deserializeChainEntry(prevObj));
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
