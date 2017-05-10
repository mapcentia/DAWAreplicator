package com.mapcentia.aws_sync;

import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by mh on 14/10/16.
 */
class Stream {
    private final String USER_AGENT = "Mozilla/5.0";

    /**
     * @throws Exception
     */
    HttpURLConnection start(String url) throws Exception {
        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();

        // optional default is GET
        con.setRequestMethod("GET");

        //add request header
        con.setRequestProperty("User-Agent", USER_AGENT);

        int responseCode = con.getResponseCode();

        return con;
    }
}
