use std::convert::TryInto;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::time::Duration;
use clap::{Arg, App};
use indicatif::ProgressBar;

extern crate ureq;

fn command_usage<'a, 'b>() -> App<'a, 'b> {
    const HTTP_CONNECT_TIMEOUT: &str = "12000";
    const HTTP_RECEIVE_TIMEOUT: &str = "12000";

    App::new("canada-climate-data-bulk-download")
    .author("Matthew Scheffel <matt@dataheck.com>")
    .about("Bulk downloads CSV data from Climate Services Canada. Will iterate over all months.")
    .arg(
        Arg::with_name("start-year")
            .long("start-year")
            .takes_value(true)
            .required(true)
            .help("First year (inclusive) to download data for")
    )
    .arg(
        Arg::with_name("end-year")
            .long("end-year")
            .takes_value(true)
            .required(true)
            .help("Last year (inclusive) to download data for")
    )
    .arg(
        Arg::with_name("station")
            .long("station")
            .takes_value(true)
            .required(true)
            .help("Station ID to bulk download for")
    )
    .arg(
        Arg::with_name("timeframe")
            .long("timeframe")
            .takes_value(true)
            .required(true)
            .help("Timeframe: hour, day, month")
    )
    .arg(
        Arg::with_name("http-connect-timeout")
            .long("http-connect-timeout")
            .takes_value(true)
            .default_value(HTTP_CONNECT_TIMEOUT)
            .help("HTTP connection timeout. Note that datamart does not use compression and has large response sizes.")
    )
    .arg(
        Arg::with_name("http-receive-timeout")
            .long("http-receive-timeout")
            .takes_value(true)
            .default_value(HTTP_RECEIVE_TIMEOUT)
            .help("HTTP receive timeout. Note that datamart does not use compression and has large response sizes.")
    )
    .arg(
        Arg::with_name("directory")
            .long("directory")
            .takes_value(true)
            .default_value(".")
            .help("The directory the bulk downloaded files should be saved to.")
    )
}

fn main() {
    let matches = command_usage().get_matches();

    let timeframe = match matches.value_of("timeframe").unwrap() {
        "hour"  => {"1"},
        "day"   => {"2"},
        "month" => {"3"},
        e => {
            panic!("Invalid timeframe specified: {}", e);
        }
    };

    let start_year = match matches.value_of("start-year").unwrap().parse::<u16>() {
        Ok(v) => {v},
        Err(_) => {
            panic!("Failed to process start_year as an integer")
        }
    };

    let end_year = match matches.value_of("end-year").unwrap().parse::<u16>() {
        Ok(v) => {v},
        Err(_) => {
            panic!("Failed to process end_year as an integer")
        }
    };
    
    let station_id = matches.value_of("station").unwrap();
    let directory = matches.value_of("directory").unwrap();

    let http_connect_timeout = matches.value_of("http-connect-timeout").unwrap().parse::<u64>().expect(&format!("Invalid http connect timeout specified: {}", matches.value_of("http-connect-timeout").unwrap()));

    let base_url = format!("https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID={station}", station=station_id);

    let bar = ProgressBar::new((12*(end_year-start_year + 1)).try_into().unwrap());

    'year: for year in start_year..=end_year {
        for month in 1..=12 {
            let target_url = format!("{base}&Year={year}&Month={month}&Day=1&time=UTC&timeframe={timeframe}&submit=%20Download+Data", base=base_url, year=year, month=month, timeframe=timeframe);

            match ureq::get(&target_url)
                .timeout(Duration::from_millis(http_connect_timeout))
                .call() {
                    Ok(response) => {
                        if !response.header("Content-Type").unwrap().starts_with("application") {
                            println!("Server did not return expected data type - check your station ID and other parameters.");
                            break 'year;
                        }
                        let path_str = format!("{directory}/{station}_{timeframe}_{year}-{month}.csv", directory=&directory, year=&year, month=&month, station=&station_id, timeframe=&timeframe);
                        let path = Path::new(&path_str);
                        let mut file = File::create(&path).unwrap();
                        
                        let mut reader = response.into_reader();
                        let mut bytes = vec![];
                        reader.read_to_end(&mut bytes).unwrap();
                        file.write_all(&bytes[..]).unwrap();
                        bar.inc(1);
                    },
                    Err(ureq::Error::Status(code, response)) => {
                        println!("{:?}", response.status_text());
                        println!("{:?}", response.get_url());
                        panic!("Failed to retrieve data from server with URL {}. Error: {}", target_url, code);
                    }
                    Err(_) => {
                        println!("I/O or transport error occured.");
                    }
            }
        }
    }

    println!("Done.");
}
