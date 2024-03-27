package gcfv2pubsub;

import com.google.cloud.functions.CloudEventsFunction;
import com.google.events.cloud.pubsub.v1.MessagePublishedData;
import com.google.events.cloud.pubsub.v1.Message;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cloudevents.CloudEvent;
import java.util.Base64;
import java.util.logging.Logger;
import java.io.File;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.commons.codec.digest.DigestUtils;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.Jwts;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import java.util.Calendar;
import java.sql.Timestamp;
import java.sql.PreparedStatement;



import java.security.Key;
import java.util.Date;


public class PubSubFunction implements CloudEventsFunction {
    private static final Logger logger = Logger.getLogger(PubSubFunction.class.getName());

    private static final String DB_URL = System.getenv("DB_URL");
    private static final String DB_USER = System.getenv("DB_USER");
    private static final String DB_PASSWORD = System.getenv("DB_PASSWORD");

    @Override
    public void accept(CloudEvent event) throws UnirestException {
        String cloudEventData = new String(event.getData().toBytes());
        Gson gson = new Gson();
        MessagePublishedData data = gson.fromJson(cloudEventData, MessagePublishedData.class);
        Message message = data.getMessage();
        String encodedData = message.getData();
        String decodedData = new String(Base64.getDecoder().decode(encodedData));

        JsonObject jsonObject = new Gson().fromJson(decodedData, JsonObject.class);
        String username = jsonObject.get("username").getAsString();
        String firstname = jsonObject.get("name").getAsString();

        logger.info("Pub/Sub message: " + decodedData);

        sendSimpleMessage(username,firstname);
    }

    public static JsonNode sendSimpleMessage(String email, String name) throws UnirestException {

        String token = generateToken(email);
        String verificationLink = generateVerificationLink(email,token);
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.MINUTE, 2);
        Timestamp expirationTime = new Timestamp(calendar.getTimeInMillis());



        String emailContent = "Hello " + name + ",\n\n"
                + "Thank you for registering with our service. To complete your registration, please click on the following link:\n\n"
                + verificationLink + "\n\n"
                + "This link will expire in 2 minutes.\n\n"
                + "If you did not register for our service, you can safely ignore this email.\n\n"
                + "Best regards,\n"
                + "CSYE6225 Class Team";


        HttpResponse<JsonNode> request = Unirest.post("https://api.mailgun.net/v3/" + "khatan.me" + "/messages")
                .basicAuth("api", "fcd7a9f21f752381cc2df2f46a2c790c-309b0ef4-b88916e7")
                .queryString("from", "Angry User <admin@khatan.me>")
                .queryString("to", email)
                .queryString("subject", "hello")
                .queryString("text", emailContent)
                .asJson();

        updateVerificationRecord(email,token,expirationTime);
        return request.getBody();
    }
    private static String generateToken(String email) {
        String token = UUID.randomUUID().toString();
        return token;
    }

    private static String generateVerificationLink(String email, String token) {
        return "http://khatan.me.:8080/v1/user/verify?token=" + token + "&email=" + email;
    }



    private static void updateVerificationRecord(String email, String token, Timestamp expirationTime) {
        logger.info("Updating email verification record...");

        String updateSQL = "UPDATE users SET token = ?, expiration_time = ?, is_email_sent = ? WHERE username = ?";

        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             PreparedStatement preparedStatement = connection.prepareStatement(updateSQL)) {

            preparedStatement.setString(1, token);
            preparedStatement.setTimestamp(2, expirationTime);
            preparedStatement.setBoolean(3, true);
            preparedStatement.setString(4, email);


            int rowsAffected = preparedStatement.executeUpdate();

            if (rowsAffected > 0) {
                logger.info("Email verification record updated successfully.");
            } else {
                logger.warning("Failed to update email verification record.");
            }

        } catch (SQLException e) {
            logger.severe("Error updating email verification record: " + e.getMessage());
        }
    }

}
