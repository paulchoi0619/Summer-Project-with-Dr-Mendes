use super::*;
use futures::channel::mpsc;
use std::{thread,time};

pub async fn new() -> Result<(mpsc::Receiver<String>,GossipLoop), Box<dyn Error>>{
    let (sender, receiver) = mpsc::channel(0);

    Ok((receiver,GossipLoop::new(sender)))
}
pub struct GossipLoop {
    gossip_sender: mpsc::Sender<String>,
}
impl GossipLoop {
    fn new(gossip_sender: mpsc::Sender<String>) -> Self {
        Self { gossip_sender }
    }
    pub async fn run(mut self){
        loop{
            let ten_sec = time::Duration::from_millis(10000);
            thread::sleep(ten_sec);
            self.gossip_sender.send("Gossip".to_string()).await;
        }
    }
}
