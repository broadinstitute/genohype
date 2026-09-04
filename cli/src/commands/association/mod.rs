mod inspect;
mod view;

use crate::cli::AssociationCommands;
use crate::Result;

pub fn run_association_command(command: AssociationCommands) -> Result<()> {
    match command {
        AssociationCommands::Inspect(args) => inspect::run(args),
        AssociationCommands::View(args) => view::run(args),
    }
}
