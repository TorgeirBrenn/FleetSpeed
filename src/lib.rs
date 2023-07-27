use reqwest;
use serde::Deserialize;
use serde_json;
use dotenv::dotenv;
use futures::stream;
use std::pin::Pin;

/**
 * This asynchronous function sends a POST request to the BarentsWatch token endpoint
 * with specific parameters ("grant_type", "client_id", "client_secret", "scope") in the form data.
 * The response is then deserialized into a TokenResponse struct (which only includes the access_token field).
 * The function finally returns the access_token as a String wrapped in a Result. In case of an error (like network failure,
 * unsuccessful status code, or invalid JSON response), the function returns an error.
 *
 * The function uses the reqwest library for HTTP requests and serde for JSON handling.
 *
 * At the time of writing, the token is valid for 1 hour but there are no guarantees as to how this may change in the future,
 * it is therefore recommended to get a new token each time the AIS API is to be called.
 *
 * # Returns
 *
 * * `Ok(String)`: On successful completion, the function returns a Result with the access_token as a String.
 * * `Err(Box<dyn std::error::Error>)`: In case of any failures (like network failure, unsuccessful status code, or invalid JSON),
 *    the function returns an error.
 */
#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    // unused fields: expires_in, token_type, scope
 }

pub async fn get_bw_token() -> Result<String, Box<dyn std::error::Error>> {
    dotenv().ok(); // Load .env variables

    let client_id = std::env::var("CLIENT_ID")
        .expect("env CLIENT_ID must be set");

    let client_secret = std::env::var("CLIENT_SECRET")
        .expect("env CLIENT_SECRET must be set");

    let client = reqwest::Client::builder()
        .build()?;

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("Content-Type", "application/x-www-form-urlencoded".parse()?);

    let mut params = std::collections::HashMap::new();
    params.insert("grant_type", "client_credentials");
    params.insert("client_id", &client_id);
    params.insert("client_secret", &client_secret);
    params.insert("scope", "ais");

    let request = client.request(reqwest::Method::POST, "https://id.barentswatch.no/connect/token")
        .headers(headers)
        .form(&params);

    let response = request.send().await?;
    let body = response.text().await?;

    match serde_json::from_str::<TokenResponse>(&body) {
        Ok(token_response) => Ok(token_response.access_token),
        Err(_e) => {
            eprintln!("Error: failed at getting token with response '{}'.", body);
            Err("Errer getting token.".into())
        },
    }
}
/**
 * Fetches a continuous data stream from a given endpoint and returns it as a `Stream` of `String`.
 *
 * This function makes a GET request to "https://live.ais.barentswatch.no/v1/ais" with proper authorization header
 * set using provided `token`, content type as "application/x-www-form-urlencoded", and creates a Stream
 * that yields chunks of the response as Strings. If an error occurs while reading a chunk from the response,
 * the error is returned in the stream.
 *
 * # Arguments
 *
 * * `token` - A string slice that holds the Bearer token.
 *
 * # Returns
 *
 * * `Ok(Pin<Box<Stream>>)` - A stream of strings which represents chunks of the response body. Can contain an error
 * if there's an error reading a chunk from the response.
 * * `Err(Box<dyn std::<error::Error>>)` - An error occurred while making the request or processing the response.
 */
pub async fn get_bw_stream(token: String) -> Result<Pin<Box<dyn futures::Stream<Item = Result<String, Box<dyn std::error::Error>>>>>, Box<dyn std::error::Error>> {
    // Prefix for the Bearer token in the authorization header
    let auth_prefix = String::from("Bearer ");
    let auth_str = auth_prefix + &token; // complete authorization string

    // Construct new reqwest client
    let client = reqwest::Client::builder().build()?;

    // Construct headers
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("Content-Type", "application/x-www-form-urlencoded".parse()?);
    headers.insert("Authorization", auth_str.parse()?);

    // Send a get request to the specified URL with headers, and await the response
    let response = client.get("https://live.ais.barentswatch.no/v1/ais")
        .headers(headers)
        .send()
        .await?;

    // Stream chunks of the response body
    // Unfold is used to generate a Stream from asynchronous closure
    let stream = stream::unfold(response, |mut res| async {

        // Obtain the next chunk from the response
        // If chunk is valid and not empty, yield it as a stream item
        // If no more chunks, end the stream
        // If an error occurs, yield the Error in the stream
        match res.chunk().await {
            Ok(Some(data)) if !data.is_empty() => Some((Ok(String::from_utf8_lossy(&data).to_string()), res)),
            Ok(_) => None,
            Err(e) => Some((Err(Box::new(e) as Box<dyn std::error::Error>), res)),
        }
    });

    // Return the stream as a boxed dynamic Stream trait object
    Ok(Box::pin(stream))
}