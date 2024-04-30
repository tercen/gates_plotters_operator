use clap::Parser;

#[derive(Parser, Default, Debug)]
#[command(author, version, about, long_about = None)]
pub(crate) struct TercenArgs {
    #[arg(short, long("taskId"))]
    pub taskId: String,
    #[arg(short, long("serviceUri"))]
    pub serviceUri: String,
    #[arg(long)]
    pub token: String,
}
