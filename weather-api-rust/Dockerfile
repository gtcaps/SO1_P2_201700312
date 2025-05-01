FROM rust:latest

WORKDIR /app

# Copiar archivos de proyecto
COPY Cargo.toml .
COPY src/ ./src/

# Compilar la aplicación
RUN cargo build --release

# Exponer puerto
EXPOSE 8080

# Configurar variable de entorno para logging
ENV RUST_LOG=info

# Ejecutar la aplicación
CMD ["./target/release/weather-api-rust"]