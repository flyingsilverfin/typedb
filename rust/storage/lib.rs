/*
 * Copyright (C) 2023 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

use rocksdb::{DB, ColumnFamilyDescriptor, Options};

pub fn hello() {
    println!("Hello from lib");

// NB: db is automatically closed at end of lifetime
    let path = "/tmp/test-db";
    {
        let db = DB::open_default(path).unwrap();
        db.put(b"my key", b"my value").unwrap();
        match db.get(b"my key") {
            Ok(Some(value)) => println!("retrieved value {}", String::from_utf8(value).unwrap()),
            Ok(None) => println!("value not found"),
            Err(e) => println!("operational problem encountered: {}", e),
        }
        db.delete(b"my key").unwrap();
    }
    let _ = DB::destroy(&Options::default(), path);


    let path = "/tmp/test-db-2";
    let mut cf_opts = Options::default();
    cf_opts.set_max_write_buffer_number(16);
    let cf = ColumnFamilyDescriptor::new("cf1", cf_opts);

    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);
    {
        let db = DB::open_cf_descriptors(&db_opts, path, vec![cf]).unwrap();
    }
    let _ = DB::destroy(&db_opts, path);

    println!("DONE!")
}
