package org.emma.spark.streaming.converter;

import com.google.gson.Gson;

import java.util.Objects;
import com.google.gson.reflect.TypeToken;

public class JsonConverter<T>{
    private final Gson gson;
    private final TypeToken<T> tokenType;

    public JsonConverter() {
        this.gson = new Gson();
        this.tokenType = new TypeToken<T>(){};
    }

    public String encode(T obj) {
        if (Objects.isNull(obj)) {
            return "";
        }
        return gson.toJson(obj);
    }

    public T decode(String json) {
        if (Objects.isNull(json) || json.length() == 0) {
            return null;
        }
        return gson.fromJson(json, tokenType.getType());
    }
}
