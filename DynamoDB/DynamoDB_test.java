///References:
///******http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStartedJavaQuery.html*****///
///******http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html*****///
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;
import java.util.TimeZone;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;


public class DynamoDB_test {

	
	
	static AmazonDynamoDBClient client = new AmazonDynamoDBClient(new ProfileCredentialsProvider());
    static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    public static void main(String[] args) throws Exception {

        try {
            
	        String forumName = "Amazon DynamoDB";
	        String threadSubject = "DynamoDB Thread 1";
	        client.setRegion(Region.getRegion(Regions.EU_WEST_1));
	
	        // Load Data
	        //uploadSampleProducts("ProductCatalog");
	        for (int i=0; i<50000 ; i++){
	        	uploadSampleProductsN("ProductCatalog", i);
	        }
	        
	        // Get an item.
	        //getBook("101", "ProductCatalog");
	        
	        // Query replies posted in the past 15 days for a forum thread.
	        //findRepliesInLast15DaysWithConfig("Reply", forumName, threadSubject);
        }  
        catch (AmazonServiceException ase) {
            System.err.println(ase.getMessage());
        }  
    }

    
    private static void getBook(String id, String tableName) {
        Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put("Id", new AttributeValue().withN(id));
        
        GetItemRequest getItemRequest = new GetItemRequest()
            .withTableName(tableName)
            .withKey(key)
            .withProjectionExpression("Id, ISBN, Title, Authors");
        
        GetItemResult result = client.getItem(getItemRequest);

        // Check the response.
        System.out.println("Printing item after retrieving it....");
        printItem(result.getItem());            
    }
    
    private static void findRepliesInLast15DaysWithConfig(String tableName, String forumName, String threadSubject) {

        String replyId = forumName + "#" + threadSubject;
        long twoWeeksAgoMilli = (new Date()).getTime() - (15L*24L*60L*60L*1000L);
        Date twoWeeksAgo = new Date();
        twoWeeksAgo.setTime(twoWeeksAgoMilli);
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        String twoWeeksAgoStr = df.format(twoWeeksAgo);
        
        Map<String, AttributeValue> lastEvaluatedKey = null;
        do {
            
            Condition hashKeyCondition = new Condition()
                .withComparisonOperator(ComparisonOperator.EQ.toString())
                .withAttributeValueList(new AttributeValue().withS(replyId));
            
            Condition rangeKeyCondition = new Condition()
                .withComparisonOperator(ComparisonOperator.GT.toString())
                .withAttributeValueList(new AttributeValue().withS(twoWeeksAgoStr));
            
            Map<String, Condition> keyConditions = new HashMap<String, Condition>();
            keyConditions.put("Id", hashKeyCondition);
            keyConditions.put("ReplyDateTime", rangeKeyCondition);
            
            QueryRequest queryRequest = new QueryRequest().withTableName(tableName)
                .withKeyConditions(keyConditions)
                .withProjectionExpression("Message, ReplyDateTime, PostedBy")
                .withLimit(1).withExclusiveStartKey(lastEvaluatedKey);   
            
           QueryResult result = client.query(queryRequest);
            for (Map<String, AttributeValue> item : result.getItems()) {
                printItem(item);
            }
            lastEvaluatedKey = result.getLastEvaluatedKey();
        } while (lastEvaluatedKey != null);        
    }
  
    private static void printItem(Map<String, AttributeValue> attributeList) {
        for (Map.Entry<String, AttributeValue> item : attributeList.entrySet()) {
            String attributeName = item.getKey();
            AttributeValue value = item.getValue();
            System.out.println(attributeName + " "
                    + (value.getS() == null ? "" : "S=[" + value.getS() + "]")
                    + (value.getN() == null ? "" : "N=[" + value.getN() + "]")
                    + (value.getB() == null ? "" : "B=[" + value.getB() + "]")
                    + (value.getSS() == null ? "" : "SS=[" + value.getSS() + "]")
                    + (value.getNS() == null ? "" : "NS=[" + value.getNS() + "]")
                    + (value.getBS() == null ? "" : "BS=[" + value.getBS() + "] \n"));
        }
    }

    private static void uploadSampleProducts(String tableName) {
        
        try {
            // Add books.
            Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
            item.put("Id", new AttributeValue().withN("101"));
            item.put("Title", new AttributeValue().withS("Book 101 Title"));
            item.put("ISBN", new AttributeValue().withS("111-1111111111"));
            item.put("Authors", new AttributeValue().withSS(Arrays.asList("Author1")));
            item.put("Price", new AttributeValue().withN("2"));
            item.put("Dimensions", new AttributeValue().withS("8.5 x 11.0 x 0.5"));
            item.put("PageCount", new AttributeValue().withN("500"));
            item.put("InPublication", new AttributeValue().withBOOL(true));
            item.put("ProductCategory", new AttributeValue().withS("Book"));
            
            PutItemRequest itemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
            client.putItem(itemRequest);
            item.clear();
            
            item.put("Id", new AttributeValue().withN("102"));
            item.put("Title", new AttributeValue().withS("Book 102 Title"));
            item.put("ISBN", new AttributeValue().withS("222-2222222222"));
            item.put("Authors", new AttributeValue().withSS(Arrays.asList("Author1", "Author2")));
            item.put("Price", new AttributeValue().withN("20"));
            item.put("Dimensions", new AttributeValue().withS("8.5 x 11.0 x 0.8"));
            item.put("PageCount", new AttributeValue().withN("600"));
            item.put("InPublication", new AttributeValue().withBOOL(true));
            item.put("ProductCategory", new AttributeValue().withS("Book"));
            
            itemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
            client.putItem(itemRequest);
            item.clear();
            
            item.put("Id", new AttributeValue().withN("103"));
            item.put("Title", new AttributeValue().withS("Book 103 Title"));
            item.put("ISBN", new AttributeValue().withS("333-3333333333"));
            item.put("Authors", new AttributeValue().withSS(Arrays.asList("Author1", "Author2")));
            // Intentional. Later we run scan to find price error. Find items > 1000 in price.            
            item.put("Price", new AttributeValue().withN("2000")); 
            item.put("Dimensions", new AttributeValue().withS("8.5 x 11.0 x 1.5"));
            item.put("PageCount", new AttributeValue().withN("600"));
            item.put("InPublication", new AttributeValue().withBOOL(true));
            item.put("ProductCategory", new AttributeValue().withS("Book"));

            itemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
            client.putItem(itemRequest);
            item.clear();

            // Add bikes.
            item.put("Id", new AttributeValue().withN("201"));
            item.put("Title", new AttributeValue().withS("18-Bike-201")); // Size, followed by some title.
            item.put("Description", new AttributeValue().withS("201 Description"));
            item.put("BicycleType", new AttributeValue().withS("Road"));
            item.put("Brand", new AttributeValue().withS("Mountain A")); // Trek, Specialized.
            item.put("Price", new AttributeValue().withN("100"));
            item.put("Gender", new AttributeValue().withS("M")); // Men's
            item.put("Color", new AttributeValue().withSS(Arrays.asList("Red", "Black")));
            item.put("ProductCategory", new AttributeValue().withS("Bicycle"));

            itemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
            client.putItem(itemRequest);
            item.clear();

            item.put("Id", new AttributeValue().withN("202"));
            item.put("Title", new AttributeValue().withS("21-Bike-202")); 
            item.put("Description", new AttributeValue().withS("202 Description"));
            item.put("BicycleType", new AttributeValue().withS("Road"));
            item.put("Brand", new AttributeValue().withS("Brand-Company A"));
            item.put("Price", new AttributeValue().withN("200"));
            item.put("Gender", new AttributeValue().withS("M"));
            item.put("Color", new AttributeValue().withSS(Arrays.asList("Green", "Black")));
            item.put("ProductCategory", new AttributeValue().withS("Bicycle"));
            
            itemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
            client.putItem(itemRequest);
            item.clear();

            item.put("Id", new AttributeValue().withN("203"));
            item.put("Title", new AttributeValue().withS("19-Bike-203")); 
            item.put("Description", new AttributeValue().withS("203 Description"));
            item.put("BicycleType", new AttributeValue().withS("Road"));
            item.put("Brand", new AttributeValue().withS("Brand-Company B"));
            item.put("Price", new AttributeValue().withN("300"));
            item.put("Gender", new AttributeValue().withS("W")); // Women's
            item.put("Color", new AttributeValue().withSS(Arrays.asList("Red", "Green", "Black")));
            item.put("ProductCategory", new AttributeValue().withS("Bicycle"));

            itemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
            client.putItem(itemRequest);
            item.clear();

            item.put("Id", new AttributeValue().withN("204"));
            item.put("Title", new AttributeValue().withS("18-Bike-204")); 
            item.put("Description", new AttributeValue().withS("204 Description"));
            item.put("BicycleType", new AttributeValue().withS("Mountain"));
            item.put("Brand", new AttributeValue().withS("Brand-Company B"));
            item.put("Price", new AttributeValue().withN("400"));
            item.put("Gender", new AttributeValue().withS("W"));
            item.put("Color", new AttributeValue().withSS(Arrays.asList("Red")));
            item.put("ProductCategory", new AttributeValue().withS("Bicycle"));

            itemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
            client.putItem(itemRequest);
            item.clear();

            item.put("Id", new AttributeValue().withN("205"));
            item.put("Title", new AttributeValue().withS("20-Bike-205")); 
            item.put("Description", new AttributeValue().withS("205 Description"));
            item.put("BicycleType", new AttributeValue().withS("Hybrid"));
            item.put("Brand", new AttributeValue().withS("Brand-Company C"));
            item.put("Price", new AttributeValue().withN("500"));
            item.put("Gender", new AttributeValue().withS("B")); // Boy's
            item.put("Color", new AttributeValue().withSS(Arrays.asList("Red", "Black")));
            item.put("ProductCategory", new AttributeValue().withS("Bicycle"));
            
            itemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
            client.putItem(itemRequest);

                
        }   catch (AmazonServiceException ase) {
            System.err.println("Failed to create item in " + tableName + " " + ase);
        } 

    }
	
    private static void uploadSampleProductsN(String tableName, int n) {
        
        try {
            // Add books.
            Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
            item.put("Id", new AttributeValue().withN( Integer.toString(n + 100) ));
            item.put("Title", new AttributeValue().withS("Book " + Integer.toString(n + 100) + " Title"));
            item.put("ISBN", new AttributeValue().withS("111-" + Integer.toString(n + 100)));
            item.put("Authors", new AttributeValue().withSS(Arrays.asList("Author1")));
            item.put("Price", new AttributeValue().withN("2"));
            item.put("Dimensions", new AttributeValue().withS("8.5 x 11.0 x 0.5"));
            item.put("PageCount", new AttributeValue().withN("500"));
            item.put("InPublication", new AttributeValue().withBOOL(true));
            item.put("ProductCategory", new AttributeValue().withS("Book"));
            
            PutItemRequest itemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
            System.out.println("Inserting item ...." + Integer.toString(n + 100) );
            client.putItem(itemRequest);
            item.clear();
            
                
        }   catch (AmazonServiceException ase) {
            System.err.println("Failed to create item in " + tableName + " " + ase);
        } 

    }

	
}
