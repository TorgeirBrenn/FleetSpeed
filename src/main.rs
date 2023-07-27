use tokio;
use fleet_speed;
use futures::StreamExt;
// use dotenv::dotenv;

/**
 * Main function that retrieves fleet speed token asynchronously and streams the speed data.
 * It uses the tokio and futures crate for processing.
 *
 * This function serves as the entry point of the program. On start, it retrieves a token from fleet_speed
 * asynchronously and handles the potential error using unwrap(). Then it uses this token to stream fleet speed
 * data. The stream is processed in chunks where each chunk is produced asynchronously. If an error occurs
 * during streaming, it is printed to the standard error.
 *
 *
 *   # Panics
 *
 *   If the function fleet_speed::get_bw_token() fails to get the token, the program will panic.
 */
#[tokio::main]
async fn main() {
    let token = fleet_speed::get_bw_token().await.unwrap();

    // uncomment lines below to instead use a token from .env to reduce number of API calls.

    // dotenv().ok(); // Load .env variables
    // let token = std::env::var("TOKEN").expect("env TOKEN must be set");

    println!("{}", token);

    // Tries to asynchronously start the stream. Handles the result using match expression
    match fleet_speed::get_bw_stream(token).await {
        // If the stream starts without errors, it is processed in chunks.
        Ok(mut stream) => {
            while let Some(chunk) = stream.next().await {
                match chunk {
                    // If a chunk is successfully produced, it is printed to the standard output.
                    Ok(text) => println!("{}", text),
                    // If an error occurs while producing a chunk, the error message is printed to standard error.
                    Err(e) => eprintln!("An error occurred while streaming: {}", e),
                }
            }
        },
        // If an error occurs while starting the stream, the error is printed to the standard error.
        Err(e) => eprintln!("An error occurred while streaming: {}", e),
    }
}