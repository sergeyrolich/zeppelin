/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.mongodb;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;
import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterPropertyBuilder;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * Mongodb Interpreter for Zeppelin.
 */
public class MongodbInterpreter extends Interpreter {

  private static Logger logger = LoggerFactory.getLogger(MongodbInterpreter.class);

  private static final String HELP = "MongoDB interpreter:\n"
          + "General format: <command> /<db>/<collection> <option> <JSON>\n"
          + "  - find /db/collection \n";

  private static final List<String> COMMANDS = Arrays.asList(
          "find");

  private static final String MONGODB_HOST = "mongodb.host";
  private static final String MONGODB_PORT = "mongodb.port";

  static {
    Interpreter.register(
            "mongodb",
            "mongodb",
            MongodbInterpreter.class.getName(),
            new InterpreterPropertyBuilder()
                    .add(MONGODB_HOST, "localhost", "The host for MongoDB")
                    .add(MONGODB_PORT, "27017", "The port for MongoDB")
                    .build());
  }

  private MongoClient client;
  private DB database;
  private DBCollection collection;
  private String host = "localhost";
  private int port = 27017;

  public MongodbInterpreter(Properties property) {
    super(property);
    this.host = getProperty(MONGODB_HOST);
    this.port = Integer.parseInt(getProperty(MONGODB_PORT));
  }

  @Override
  public void open() {
    client = new MongoClient(host, port);
  }


  @Override
  public void close() {
    if (client != null) {
      client.close();
    }
  }

  @Override
  public InterpreterResult interpret(String cmd, InterpreterContext interpreterContext) {
    logger.info("Run MongoDB command '" + cmd + "'");

    if (StringUtils.isEmpty(cmd) || StringUtils.isEmpty(cmd.trim())) {
      return new InterpreterResult(InterpreterResult.Code.SUCCESS);
    }

    if (client == null) {
      return new InterpreterResult(InterpreterResult.Code.ERROR,
              "Problem with the MongoDB client, please check your configuration (host, port,...)");
    }

    String[] items = StringUtils.split(cmd.trim(), " ", 3);

    if ("help".equalsIgnoreCase(items[0])) {
      return processHelp(InterpreterResult.Code.SUCCESS, null);
    }

    if (items.length < 2) {
      return processHelp(InterpreterResult.Code.ERROR, "Arguments missing");
    }

    final String method = items[0];
    final String url = items[1];
    final String data = items.length > 2 ? items[2].trim() : null;

    final String[] urlItems = StringUtils.split(url.trim(), "/");

    try {
      if ("find".equalsIgnoreCase(method)) {
        return processFind(urlItems, data);
      }
      return processHelp(InterpreterResult.Code.ERROR, "Unknown command");
    }
    catch (Exception e) {
      return new InterpreterResult(InterpreterResult.Code.ERROR, "Error : " + e.getMessage());
    }
  }


  @Override
  public int getProgress(InterpreterContext interpreterContext) {
    return 0;
  }

  @Override
  public List<InterpreterCompletion> completion(String s, int i) {
    final List suggestions = new ArrayList<>();

    if (StringUtils.isEmpty(s)) {
      suggestions.addAll(COMMANDS);
    } else {
      for (String command : COMMANDS) {
        if (command.toLowerCase().contains(s)) {
          suggestions.add(command);
        }
      }
    }
    return suggestions;
  }

  private InterpreterResult processHelp(InterpreterResult.Code code, String additionalMessage) {
    final StringBuilder buffer = new StringBuilder();

    if (additionalMessage != null) {
      buffer.append(additionalMessage).append("\n");
    }

    buffer.append(HELP).append("\n");

    return new InterpreterResult(code, InterpreterResult.Type.TEXT, buffer.toString());
  }


  private InterpreterResult processFind(String[] urlItems, String data) {

    if (urlItems.length != 2
            || StringUtils.isEmpty(urlItems[0])
            || StringUtils.isEmpty(urlItems[1])) {
      return new InterpreterResult(InterpreterResult.Code.ERROR,
              "Bad URL (it should be /db/collection)");
    }

    DBObject query = (DBObject) JSON.parse(data);

    database = client.getDB(urlItems[0]);
    collection = database.getCollection(urlItems[1]);
    List<DBObject> obj = collection.find(query).toArray();

    String json = "";

    if (obj.size() > 0) {
      StringBuilder row = new StringBuilder();
      for (int i = 0; i < obj.size(); i++){
        if (i == 0) {
          row.append(formTable(obj.get(i).toString(), true));
        } else {
          row.append(formTable(obj.get(i).toString(), false));
        }
        json = row.toString();
      }

      return (new InterpreterResult(
              InterpreterResult.Code.SUCCESS,
              InterpreterResult.Type.TABLE,
              json));

    }
    return new InterpreterResult(InterpreterResult.Code.ERROR, "Document not found");
  }

  private String formTable(String json, boolean includeHeader){
    StringBuffer ret = new StringBuffer();

    Map<String, Object> retMap = new Gson()
            .fromJson(json, new TypeToken<HashMap<String, Object>>() {}.getType());

    if (includeHeader){
      for (Map.Entry<String, Object> entry : retMap.entrySet()) {
        ret = ret.append(entry.getKey()).append("\t");
      }
      ret.append("\n");
    }
    for (Map.Entry<String, Object> entry : retMap.entrySet()) {
      ret.append(entry.getValue()).append("\t");
    }
    ret.append("\n");
    return ret.toString();
  }

  @Override
  public void cancel(InterpreterContext arg0) {
      // Nothing to do

  }

  @Override
  public FormType getFormType() {

    return FormType.SIMPLE;
  }
}
