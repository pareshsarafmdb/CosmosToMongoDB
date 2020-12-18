package com.psaraf.cosmostomongo;

import org.bson.Document;

import java.util.ArrayList;

public interface DataSource {

	void Connect(String user, String pass);

	void RunQuery(String sql, ArrayList<String> params, Document parent);

}