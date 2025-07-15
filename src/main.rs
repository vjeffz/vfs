use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use vfs::VFS;

#[derive(Parser)]
#[command(name = "vfs")]
#[command(about = "Virtual File Splitter - Encode files as base64 chunks in S3 object keys")]
#[command(version = "0.1.0")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Encode a file into base64 chunks stored as S3 object keys
    Encode {
        /// Input file path
        input_file: String,
        /// S3 URI (s3://bucket/prefix/)
        s3_uri: String,
        /// Force overwrite existing data without prompting
        #[arg(long)]
        force: bool,
    },
    /// Restore a file from S3 object keys
    Restore {
        /// S3 URI (s3://bucket/prefix/)
        s3_uri: String,
        /// Output file path
        output_file: String,
    },
    /// Delete all objects under an S3 prefix
    Delete {
        /// S3 URI (s3://bucket/prefix/)
        s3_uri: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let vfs = VFS::new().await?;

    match cli.command {
        Commands::Encode { input_file, s3_uri, force } => {
            vfs.encode(&input_file, &s3_uri, force).await?;
        }
        Commands::Restore { s3_uri, output_file } => {
            vfs.restore(&s3_uri, &output_file).await?;
        }
        Commands::Delete { s3_uri } => {
            vfs.delete(&s3_uri).await?;
        }
    }

    Ok(())
}