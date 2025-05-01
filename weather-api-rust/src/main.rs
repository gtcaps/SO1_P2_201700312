use actix_cors::Cors;
use actix_web::{middleware, web, App, HttpResponse, HttpServer, Responder};
use log::{info, warn};
use serde::{Deserialize, Serialize};
use std::env;


#[derive(Debug, Serialize, Deserialize)]
struct WeatherData {
    description: String,
    country: String,
    weather: String,
}

// post
async fn receive_weather(data: web::Json<WeatherData>) -> impl Responder {
    info!(
        "Recibido: {} en {} - {}",
        data.description, data.country, data.weather
    );

    HttpResponse::Ok().json(serde_json::json!({
        "status": "success",
        "message": "Datos del clima recibidos correctamente"
    }))
}

// health
async fn health_check() -> impl Responder {
    HttpResponse::Ok().body("Healthy!")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env::set_var("RUST_LOG", "info");
    env_logger::init();

    let port = env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let addr = format!("0.0.0.0:{}", port);

    info!("Iniciando servidor en {}", addr);

    HttpServer::new(|| {
        // Configurar CORS
        let cors = Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header();

        App::new()
            .wrap(middleware::Logger::default())
            .wrap(cors)
            .service(
                web::resource("/input")
                    .route(web::post().to(receive_weather)),
            )
            .route("/health", web::get().to(health_check))
    })
    .bind(addr)?
    .run()
    .await
}