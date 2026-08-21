//! The scripted caller: the drain window, made visible from outside.
//!
//! This is the client, and it is deliberately not a client *library*. It opens
//! sockets, writes one line, reads one line, and prints what came back. There
//! is no retry, no reconnection and no framing to learn, because every minute a
//! reader spends on the wire format is a minute not spent on the thing the
//! example proves.
//!
//! # What it demonstrates, in the order it demonstrates it
//!
//! 1. **A served request.** One line in, one line out, so the refusals below
//!    read against something.
//! 2. **Backpressure.** More requests at once than the bench admits. The extra
//!    ones come back `busy` — refused at the door, with the refusal on the
//!    wire. A queue that grew instead would answer all six `ok` and look
//!    better here, which is exactly how an unbounded queue survives review and
//!    is found later, in production, by the memory it holds.
//! 3. **The window.** One request is put in flight and one connection is opened
//!    and left silent. Then the stop is requested, and three facts are printed
//!    in the order they become true:
//!    * the door is shut — [`Doorway::is_open`] goes false the moment the
//!      acceptor reacts to `Draining`;
//!    * a *new* connection is refused, by the operating system, with no process
//!      code involved;
//!    * the request that was already in flight **finishes and its caller reads
//!      a real answer**, several hundred milliseconds after the door shut.
//!
//! The third bullet is the whole example. A process with one stop signal
//! instead of two cannot print it: the only way to refuse the new caller would
//! be to drop the old one.
//!
//! The silent connection is a fourth fact, and a subtler one. It was accepted
//! before `Draining` and had asked for nothing yet, and it is still held and
//! still answered: what the window protects is the whole *conversation*, not
//! only the request that was already admitted.
//!
//! Whether its late line is served or refused depends on which door shut
//! first. The socket is closed by the acceptor the instant `Draining` fires;
//! the queue's door is closed by the runnable that works it, which only comes
//! up for air between jobs. Under the settings this application ships the hand
//! is holding a 250 ms job at that moment, so the late line is admitted and
//! answered `ok` — a moment later it would have read `closing`. Both are
//! answers. Neither door replies with a reset, and that is the property worth
//! having.
//!
//! # Why every wait here is bounded
//!
//! [`demonstrate`] wraps the whole script in [`BUDGET`] and asks for the stop
//! whatever happens. A demonstration that can wedge is worse than no
//! demonstration: `cargo run -p service` must always terminate, including when
//! the thing being demonstrated has regressed.

use core::time::Duration;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use gateway_bundle::Doorway;
use kernel::KernelHandle;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::task::JoinSet;
use tokio::time::timeout;

use crate::timeline::Clock;

/// Everything this script may take, from the first connection to the last line.
///
/// The bound belongs here rather than in `main` because this is the unit that
/// can hang: it talks to sockets. `main` joins it with the same number.
pub const BUDGET: Duration = Duration::from_secs(20);

/// How many requests are fired at once to make the bench refuse.
///
/// Larger than the capacity this application configures, and by more than one,
/// so the refusal does not depend on how quickly the hand picks the first job
/// up.
const BURST: usize = 6;

/// How long the script waits for what it just sent to be admitted.
///
/// The gateway hands a line to the handler on its own task, so "the request is
/// in flight" is not true the instant `write` returns. Nothing public
/// publishes that moment, so the script waits a little instead — see
/// [`crate`]'s notes on what the public surface does not offer.
const SETTLE: Duration = Duration::from_millis(80);

/// How often the script looks at the door while waiting for it to shut.
const GLANCE: Duration = Duration::from_millis(2);

/// How many glances before the script gives up on the door ever shutting.
///
/// The product with [`GLANCE`] is the bound: one second, which is far longer
/// than the acceptor takes and far shorter than a wedged suite.
const GLANCES: u32 = 500;

/// Runs the script, then makes sure the process stops either way.
///
/// The stop request is repeated after the script because
/// [`KernelHandle::shutdown`] is idempotent and because the script may have
/// ended early: a failed connection must still leave a process that exits.
pub async fn demonstrate(doorway: Arc<Doorway>, handle: KernelHandle, clock: Arc<Clock>) {
    match timeout(BUDGET, script(&doorway, &handle, &clock)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => clock.say(&format!("the script stopped early: {error}")),
        Err(_) => clock.say(&format!("the script overran {BUDGET:?}")),
    }
    handle.shutdown();
}

/// The script itself, in the order a reader should meet it.
async fn script(doorway: &Doorway, handle: &KernelHandle, clock: &Clock) -> io::Result<()> {
    // Port zero was configured, so the bound address is not knowable before
    // boot. The component publishes it and this waits for it, which is the
    // difference between reading a fact and racing one.
    let Some(address) = doorway.opened().await else {
        clock.say("the door never opened");
        return Ok(());
    };
    clock.say(&format!("serving on {address}"));

    // ---- 1. what a served request looks like ---------------------------
    clock.say(&format!("served   {}", ask(address, "hello").await?));

    // ---- 2. backpressure ------------------------------------------------
    let answers = burst(address).await;
    let refused = answers.iter().filter(|line| line.ends_with("busy")).count();
    for answer in &answers {
        clock.say(&format!("burst    {answer}"));
    }
    clock.say(&format!(
        "{refused} of {BURST} refused: the bench is bounded and said so on the wire"
    ));

    // ---- 3. the window ---------------------------------------------------
    let mut working = Held::open(address).await?;
    working.say("in-flight").await?;
    let mut silent = Held::open(address).await?;
    tokio::time::sleep(SETTLE).await;
    clock.say("one request is in flight; one connection is open and has said nothing");

    handle.shutdown();

    if shut(doorway).await {
        clock.say("the door is shut — the acceptor stopped accepting at Draining");
    } else {
        clock.say("the door is STILL OPEN — the acceptor did not react to Draining");
    }

    match Held::open(address).await {
        Err(error) => clock.say(&format!("a new caller is refused: {error}")),
        Ok(_) => clock.say("a new caller was ACCEPTED after Draining — the window leaked"),
    }

    silent.say("late").await?;
    clock.say(&format!("held     {}", silent.hear().await?));

    // The line this whole example is about: accepted before the stop,
    // answered after it, with a real reply rather than a reset.
    clock.say(&format!("window   {}", working.hear().await?));

    Ok(())
}

/// Waits for the door to shut, and says whether it did.
///
/// [`Doorway::is_open`] is the public fact that the acceptor reacted: the
/// component owns the socket, the runnable closes it on the way into the drain
/// window, and both are visible from out here without a hook of any kind.
///
/// Bounded by construction — [`GLANCES`] times [`GLANCE`] — so a regression
/// that leaves the door open fails this line instead of parking the process.
async fn shut(doorway: &Doorway) -> bool {
    for _ in 0..GLANCES {
        if !doorway.is_open() {
            return true;
        }
        tokio::time::sleep(GLANCE).await;
    }
    false
}

/// Fires [`BURST`] requests at once and collects what came back.
///
/// Concurrently, and that matters: sent one after another they would all be
/// served, because the bench empties between them. Backpressure is a statement
/// about simultaneity.
async fn burst(address: SocketAddr) -> Vec<String> {
    let mut asking: JoinSet<io::Result<String>> = JoinSet::new();
    for index in 0..BURST {
        asking.spawn(async move { ask(address, &format!("burst-{index}")).await });
    }

    let mut answers = Vec::with_capacity(BURST);
    while let Some(joined) = asking.join_next().await {
        answers.push(match joined {
            Ok(Ok(line)) => line,
            Ok(Err(error)) => format!("(no answer: {error})"),
            Err(error) => format!("(the asking task ended: {error})"),
        });
    }

    // The order requests are accepted in is the operating system's business,
    // so the lines are sorted before they are printed. A demonstration whose
    // output changes between runs teaches a reader to ignore it.
    answers.sort();
    answers
}

/// One request on a connection of its own: open, say, hear, close.
async fn ask(address: SocketAddr, line: &str) -> io::Result<String> {
    let mut held = Held::open(address).await?;
    held.say(line).await?;
    held.hear().await
}

/// A connection the script keeps open across a phase change.
///
/// The whole client. It exists because the interesting requests are the ones
/// still open when the ladder moves, and [`ask`] — which opens and closes in
/// one call — cannot hold one.
struct Held {
    /// The socket, buffered so a reply can be read a line at a time.
    wire: BufReader<TcpStream>,
}

impl Held {
    /// Connects.
    ///
    /// The error this returns after `Draining` is the demonstration: the
    /// listening socket is gone, so the refusal comes from the operating
    /// system and no code in this process runs at all.
    async fn open(address: SocketAddr) -> io::Result<Self> {
        Ok(Self {
            wire: BufReader::new(TcpStream::connect(address).await?),
        })
    }

    /// Writes one line and flushes it.
    async fn say(&mut self, line: &str) -> io::Result<()> {
        let socket = self.wire.get_mut();
        socket.write_all(line.as_bytes()).await?;
        socket.write_all(b"\n").await?;
        socket.flush().await
    }

    /// Reads one line back.
    ///
    /// An empty read is end of file: the process went away without answering.
    /// It is rendered rather than returned as an error, because "the connection
    /// was reset" is exactly the outcome the drain window exists to avoid and a
    /// reader should see it named when it happens.
    async fn hear(&mut self) -> io::Result<String> {
        let mut line = String::new();
        if self.wire.read_line(&mut line).await? == 0 {
            return Ok("(reset — no answer)".to_owned());
        }
        Ok(line.trim_end().to_owned())
    }
}
