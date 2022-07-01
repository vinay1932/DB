import java.sql.Connection;  
import java.sql.DatabaseMetaData;  
import java.sql.DriverManager;  
import java.sql.SQLException;  
import java.sql.Statement;  
import java.sql.PreparedStatement;   
import java.sql.ResultSet; 


public class DBQuery {
	// DataBase Creation
	public static void createNewDatabase(String fileName) {  
		// SQLite connection string   
        String url = "jdbc:sqlite:C:/sqlite/" + fileName;  
   
        try {  
            Connection conn = DriverManager.getConnection(url);  
            if (conn != null) {  
                DatabaseMetaData meta = conn.getMetaData();  
                System.out.println("The driver name is " + meta.getDriverName());  
                System.out.println("A new database has been created.\n\n");  
            }  
   
        } catch (SQLException e) {  
            System.out.println(e.getMessage());  
        }  
    }  
	
	// Table Creation
	public static void createNewTable() {  
          
        String url = "jdbc:sqlite:C://sqlite/MuleSoft.db";  
          
        // SQL statement for creating a new table  
        String sql =  "CREATE TABLE IF NOT EXISTS Movies (\n"  
                + " id integer PRIMARY KEY,\n"  
                + " movie_name text NOT NULL,\n"
                + " actor text,\n"
                + " actress text,\n"
                + " year integer NOT NULL,\n"
                + " director text NOT NULL\n"
                + ");"; 
          
        try{  
            Connection conn = DriverManager.getConnection(url);  
            Statement stmt = conn.createStatement();  
            stmt.execute(sql);  
        } catch (SQLException e) {  
            System.out.println(e.getMessage());  
        }  
    } 
	
	// Inserting Data into Table
	public static void insert(int id, String movie_name, String actor, String actress, int year, String director) {  
		
		String url = "jdbc:sqlite:C://sqlite/MuleSoft.db";
		
		// SQL statement for inserting data into table
		String sql = "INSERT INTO Movies(id, movie_name, actor, actress, year, director) VALUES(?,?,?,?,?,?)";  
   
        try{  
        	Connection conn = DriverManager.getConnection(url);   
            PreparedStatement pstmt = conn.prepareStatement(sql);  
            pstmt.setInt(1, id);
            pstmt.setString(2, movie_name);  
            pstmt.setString(3, actor);
            pstmt.setString(4, actress);
            pstmt.setInt(5, year);
            pstmt.setString(6, director);
            pstmt.executeUpdate();  
        } catch (SQLException e) {  
            System.out.println(e.getMessage());  
        }  
    }
	
	// Query to retrieve all rows from the table
	public static void selectAll(){  
		
		String url = "jdbc:sqlite:C://sqlite/MuleSoft.db";  
		
		// SQL statement for retrieving data from the table
        String sql = "SELECT * FROM Movies";  
          
        try {  
        	Connection conn = DriverManager.getConnection(url);   
            Statement stmt  = conn.createStatement();  
            ResultSet rs    = stmt.executeQuery(sql); 
            // loop through the result set  
            while (rs.next()) {  
                System.out.println("Movie " + rs.getInt("id") + ":"); 
                System.out.println("\t" + "Name: " + rs.getString("movie_name") + "\n" +
                					"\t" + "Actor: " + rs.getString("actor") + "\n" +
                					"\t" + "Actress: " + rs.getString("actress") + "\n" +
                					"\t" + "Year Of Release: " + rs.getInt("year") + "\n" +
                					"\t" + "Director: " + rs.getString("director") + "\n---------------------------------------------"); 
            }  
        } catch (SQLException e) {  
            System.out.println(e.getMessage());  
        }  
    }
	
	// Query to select movies based on the actor's name
	public static void selectQuery(){

		String url = "jdbc:sqlite:C://sqlite/MuleSoft.db";  
		
		// SQL statement for retrieving data from the table based on actor name
        String sql = "SELECT * FROM Movies WHERE actor = 'NTR'";  
          
        try {  
        	Connection conn = DriverManager.getConnection(url); 
            Statement stmt  = conn.createStatement();  
            ResultSet rs    = stmt.executeQuery(sql); 
            // loop through the result set  
            while (rs.next()) {  
                System.out.println("Movie " + rs.getInt("id") + ":"); 
                System.out.println("\t" + "Name: " + rs.getString("movie_name") + "\n" +
                					"\t" + "Actor: " + rs.getString("actor") + "\n" +
                					"\t" + "Actress: " + rs.getString("actress") + "\n" +
                					"\t" + "Year Of Release: " + rs.getInt("year") + "\n" +
                					"\t" + "Director: " + rs.getString("director") + "\n---------------------------------------------"); 
            }  
        } catch (SQLException e) {  
            System.out.println(e.getMessage());  
        }  
    }
	
  
    public static void main(String[] args) {  
    	
        createNewDatabase("MuleSoft.db");
        
        createNewTable();
        
        insert(1, "BOMMARILLU", "SIDDHARTH", "Jenelia", 2005, "Bhaskar");  
        insert(2, "Bahubali2", "Prabhas", "Anushka", 2015, "Rajamouli"); 
        insert(3, "KGF", "Yash", "Srindhi Shetty", 2022, "Prashanth Neel");  
        insert(4, "Padmaavat", "Shahid Kapoor", "Deepika Padukone", 2018, "Sanjay Leela Bhansali"); 
        insert(5, "Shershaah", "Sidharth Malhotra", "Kiara Advani", 2021, "Vishnuvardhan");
        insert(6, "Shyamsingharoy", "Nani", "Saipallavi", 2019, "Gowtam Tinnanuri");
        insert(7, "Avatar", "Sam Worthington", "Zoe Saldana", 2009, "James Cameron");
        insert(8, "Dhruva", "Ram Charan", "Rakul Preet Singh", 2016, "Surender Reddy");
        insert(9, "Jai Bhim", "Suriya", "Lijomol Jose", 2021, "TJ Gnanavel");
        insert(10, "Vikram", "kamal hassan", "Taapsee", 2016, "Lokesh");
        
        selectAll();
        System.out.println("\n\n=====================================================\n\n");
        
        selectQuery();
        
    } 
}