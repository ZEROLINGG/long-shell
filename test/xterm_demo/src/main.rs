use std::sync::Arc;

use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
    Router,
};
use shell_engine::Shell;
use tokio::sync::{mpsc, Mutex};
use tower_http::services::ServeDir;

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/ws", get(ws_handler))
        .fallback_service(ServeDir::new("static"));

    let addr = "0.0.0.0:3000";
    println!("listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind failed");
    axum::serve(listener, app)
        .await
        .expect("server error");
}

async fn ws_handler(ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(handle_socket)
}

async fn handle_socket(mut socket: WebSocket) {
    let (output_tx, mut output_rx) = mpsc::channel::<String>(256);

    let shell = match Shell::new("bash")
        .enable_pty()
        .enable_buffer()
        .on_output({
            let tx = output_tx;
            move |chunk| {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(chunk).await;
                }
            }
        })
        .spawn()
        .await
    {
        Ok(s) => Arc::new(Mutex::new(s)),
        Err(e) => {
            let _ = socket
                .send(Message::Text(format!("spawn failed: {e}").into()))
                .await;
            return;
        }
    };

    loop {
        tokio::select! {
            msg = output_rx.recv() => {
                match msg {
                    Some(chunk) => {
                        if socket.send(Message::Text(chunk.into())).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Some((cols, rows)) = parse_resize(&text) {
                            let mut sh = shell.lock().await;
                            let _ = sh.resize(cols, rows).await;
                        } else {
                            let mut sh = shell.lock().await;
                            let _ = sh.send(&text).await;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
        }
    }

    let mut sh = shell.lock().await;
    let _ = sh.exit().await;
}

fn parse_resize(msg: &str) -> Option<(u16, u16)> {
    let msg = msg.trim();
    let prefix = "{\"resize\":[";
    let suffix = "]}";
    if msg.starts_with(prefix) && msg.ends_with(suffix) {
        let inner = &msg[prefix.len()..msg.len() - suffix.len()];
        let mut parts = inner.split(',');
        let cols = parts.next()?.trim().parse().ok()?;
        let rows = parts.next()?.trim().parse().ok()?;
        Some((cols, rows))
    } else {
        None
    }
}
