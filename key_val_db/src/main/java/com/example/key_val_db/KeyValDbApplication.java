package com.example.key_val_db;

import com.example.key_val_db.datastore.KeyValDataStore;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.IOException;

@SpringBootApplication
@EnableScheduling
public class KeyValDbApplication {

	public static void main(String[] args) throws IOException {
		SpringApplication.run(KeyValDbApplication.class, args);
		/**System.out.println(dataStore.get("Prod101", "amazon", "products"));
		System.out.println(dataStore.get("Vivek10101", "amazon", "cart"));
		dataStore.put("Vishal10601", "T-shirts:2, bottles:1", "amazon", "cart");
		System.out.println(dataStore.get("Vishal10601", "amazon", "cart"));

		dataStore.createDB("swiggy");
		dataStore.createTable("swiggy", "restaurants");
		dataStore.deleteTable("ppa", "users");
		 **/
	}

}
