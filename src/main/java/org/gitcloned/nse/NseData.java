package org.gitcloned.nse;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.gitcloned.nse.http.NseHTTP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

public class NseData {

    Logger logger = LoggerFactory.getLogger(NseData.class);

    private final NseHTTP requestBuilder;

    public NseData(NseHTTP requestBuilder) {
        this.requestBuilder = requestBuilder;
    }

    public NseHTTP getRequestBuilder() {
        return requestBuilder;
    }

    private boolean isSuccessfulResponse (int statusCode) {

        if (statusCode - 200 < 0) return false;
        if (statusCode - 200 >= 100) return false;
        return true;
    }

    public JsonArray  scanTable (String groupName, String tableName) throws IOException, RuntimeException {

//        StringBuilder api = new StringBuilder("");
//        api.append(tableName);
//        api.append("StockWatch.json");
//
//
//        HttpRequest request = this.requestBuilder.buildGetRequest(api.toString(), null);
//        request.setConnectTimeout(2000);
//
//        System.out.println("******************************************************************");
//        System.out.println(request.getRequestMethod().toString());
//        System.out.println("******************************************************************");


//        System.out.println( "\u001B[33m" + "Inside scan Table method  after building request "+"\u001B[0m");
//
//        //HttpResponse response = request.setReadTimeout(20000).execute();
//        System.out.println( "\u001B[33m" + "Inside scan Table method  after request execute "+"\u001B[0m");


//        if (!isSuccessfulResponse(response.getStatusCode())) {
//            throw new RuntimeException("Data Request Failed, status code (" + response.getStatusCode() + "), Response: " + response.parseAsString());
//        }

  //      System.out.println( "\u001B[33m" + "Inside scan Table method  after successful response status code "+"\u001B[0m");



    //      Post response
        String token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2MiOiJmNzk0ZmY4NmZiYzgxMWVhYmQ2YzNhOTExNDNlM2Q0MiIsImF1ZCI6Imh0dHBzOi8vbWFuYWdlLnNreWZsb3dhcGlzLmRldiIsImV4cCI6MTY3MTI2MjQ0MywiaWF0IjoxNjY4NjcwNDQzLCJpc3MiOiJzYS1hdXRoQG1hbmFnZS5za3lmbG93YXBpcy5kZXYiLCJqdGkiOiJyODc1ZmU0NjZhODM0MGZkYjBjNjVjMjU4MTIxOTdmMiIsInN1YiI6ImUzZWQ2YmE5OWY1NjQ1YzNiZjZmZGRhYmJiYmYzYzYyIn0.FtIfLDxcslq2fu-fMJLTGaSXlkKDd2Y6n0gUqwHbrAosYSAdSd1vEpaMaetor4rpEIpweFp6KhscEQ0sAITppG0O_sUFMpfuzW-MTmqQL3m4IxrN50ryCghrnKroDAXP6Z0ZehYF27y4fhlYwQBw3gaA-YqygzpF3nkNutB2mWdAWuIBjvu5omUqk1aVHdWFC-d2KkQt2YJLuWbbaXb162K6JBavo03yQtAzqhQdppwZnUxZ6lUndWXo5-1rav8J-AdqbXxt9774yKcC7wVBu69XqGN7vosgFW9B7vyT8cz_lZmurbgsKOq-YukBfWlBqqUy3mhpQyh4ZTmth1n4kw'";
        URL url = new URL("https://sb.area51.vault.skyflowapis.dev/v1/vaults/h54b9fa800cc4916974fdc7407463783/query");
        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        conn.setRequestMethod("POST");
        //String authString = "Bearer " + Base64.getEncoder().withoutPadding().encodeToString(token.getBytes("utf-8"));
        String authString = "Bearer " + token;
        conn.setRequestProperty("Authorization", authString);
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);
//
        String jsonInputString = "{ \"query\": \"select * from medicare;\"}";
//
        try(OutputStream os = conn.getOutputStream()) {
            byte[] input = jsonInputString.getBytes("utf-8");
            os.write(input, 0, input.length);
        }
//        //conn.getOutputStream().write(postDataBytes);
        int responseCode = conn.getResponseCode();
        System.out.println("POST Response Code :: " + responseCode);
        Reader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
        StringBuilder sb = new StringBuilder();
        for (int c; (c = in.read()) >= 0;)
            sb.append((char)c);
        String responsePost = sb.toString();
//        //System.out.println("**********************");
       // System.out.println(responsePost);


        //JSONObject myResponse = new JSONObject(response.toString());
//        System.out.println("result after Reading JSON Response");
//        System.out.println("origin- "+myResponse.getString("origin"));
//        System.out.println("url- "+myResponse.getString("url"));
//        JSONObject form_data = myResponse.getJSONObject("form");
//        System.out.println("CODE- "+form_data.getString("CODE"));
//        System.out.println("email- "+form_data.getString("email"));
//        System.out.println("message- "+form_data.getString("message"));
//        System.out.println("name"+form_data.getString("name"));



//        String responseString = response.parseAsString();
//
        JsonParser parser = new JsonParser();

        // Post request to json tree
        JsonElement postTree = parser.parse(responsePost.toString());

       // System.out.println(postTree);
        JsonArray postData = null;

        if(postTree.isJsonObject()) {
            JsonElement postRows = postTree.getAsJsonObject().get("records");
            System.out.println("++++++++++++++++++++++++++++++++++++++++++++");
            System.out.println("Rows ---> " + postRows);


            if(postRows.isJsonArray()){
                System.out.println("PostRows is a json array ..................");
                postData = postRows.getAsJsonArray();
                System.out.println("&&&&&&&&&&&&&&&&&&&  "+postData.size()+" *************************************%%%%%%%%%%%%%%%%%%%%%%%%%%");
                System.out.println(postData.get(0));
                System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
                System.out.println(postData.get(1));

               // System.out.println(postData.getAsJsonObject());
                for (JsonElement postRow : postData) {
                    //System.out.println("each row data -->" +postRow.toString());
                    }

            }


        }

        /////////////////////////////

        //JsonElement tree = parser.parse(responseString);
        JsonElement tree = parser.parse(new FileReader("/home/deq/IdeaProjects/CalciteRestAPIDynamicSchema/src/main/resources/response.json"));

        if (tree.isJsonObject()) {

            System.out.println( "\u001B[33m" + "tree is json object "+"\u001B[0m");

            JsonElement rows = tree.getAsJsonObject().get("data");
            JsonElement time = tree.getAsJsonObject().get("time");

            if (time.isJsonNull()) {
                throw new RuntimeException("Data Request Failed: Response is not a valid json, cannot find the 'time' of the data response");
            }

            String timeString = time.getAsString();

            logger.debug(String.format("fetched data as of time: '%s'", time));

            if (rows.isJsonArray()) {

                JsonArray data = rows.getAsJsonArray();
//
//                System.out.println("&&&&&&&&&&&&&&&&&&&  "+data.size()+" *************************************%%%%%%%%%%%%%%%%%%%%%%%%%%");
//                System.out.println(data.get(0));
//                System.out.println("**************************^^^^^^^^^^^^^^^^^^^^^");
//                System.out.println(data.get(1));
//                System.out.println("**************************^^^^^^^^^^^^^^^^^^^^^");


                for (JsonElement row : data) {

                   // System.out.println("each nse row data -->" +row.toString());
                    if (row.isJsonObject()) {

                        ((JsonObject)row).addProperty("time", timeString);
                    } else {
                        throw new RuntimeException("Data Request Failed: Response is not a valid json, each row in 'data' should be a valid json object");
                    }
                }

                //return data;
                return postData;

            }
            else {
                throw new RuntimeException("Data Request Failed: Response is not a valid json, should have 'data' as array of json objects");
            }
        } else {
            throw new RuntimeException("Data Request Failed: Response is not a valid json, should be a json object");
        }
    }
}
