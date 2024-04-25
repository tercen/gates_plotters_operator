use clap::Parser;

// var args = <String>[
// '--taskId',
// task.id,
// '--serviceUri',
// serviceUri,
// '--token',
// params.token
// ];

#[derive(Parser, Default, Debug)]
#[command(author, version, about, long_about = None)]
pub(crate) struct TercenArgs {
    /// Ip to listen to
    #[arg(short, long)]
    pub taskId: String,

    /// Port
    #[arg(short, long)]
    pub serviceUri: String,

    /// Ip to listen to
    #[arg(long)]
    pub token: String,
}
