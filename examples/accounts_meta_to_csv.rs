use itertools::Itertools;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::path::PathBuf;

pub fn main() {
    let accounts_meta_file =
        PathBuf::from("/Users/stefan/mango/projects/geyser-misc/ledger-debug-accounts.txt");

    let file = File::open(accounts_meta_file).expect("file must exist");
    let reader = io::BufReader::new(file);
    for blocks in &reader.lines().chunks(9) {
        let blocks = blocks.collect_vec();
        let account_pk = blocks[0].as_ref().unwrap().replace(":", "");
        if account_pk == "" {
            break;
        }
        let owner_pk = blocks[2].as_ref().unwrap();
        let ltick = owner_pk.find("'");
        let rtick = owner_pk.rfind("'");
        let owner_pk = &owner_pk[ltick.unwrap() + 1..rtick.unwrap()];

        let data_len = blocks[6].as_ref().unwrap().replace("  data_len: ", "");

        println!("{};{};{}", account_pk, owner_pk, data_len);
    }
}

/*
16FMCmgLzCNNz6eTwGanbyN2ZxvTBSLuQ6DZhgeMshg:
  balance: 0.00095352 SOL
  owner: 'Feature111111111111111111111111111111111111'
  executable: false
  slot: 0
  rent_epoch: 0
  data_len: 9
  data: 'AQAAAAAAAAAA'
  encoding: "base64"
*/
