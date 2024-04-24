// This script is invoked like this: node insertPemsHourlyBulk.js RAW/pems station data month

import { Command } from "commander";
let program = new Command();

program.option("-f", "file path");

import path from "path";
import _ from "lodash";
import fs from "fs/promises";
import { DateTime } from "luxon";

import { parse } from "csv-parse";

program.parse();
const options = program.opts();
const limit = options.first ? 1 : undefined;

const cmdOptions = program.args[0].split(options.separator, limit);

const filePath = cmdOptions[0];

if (!filePath) {
  console.log("path to file required - relative from directory");
  process.exit();
}

// Hours to define peak periods
const am_peak_start = 6;
const am_peak_end = 9;
const pm_peak_start = 16;
const pm_peak_end = 19;

const stations = [
  {
    name: "Antioch",
    code: "ANTC",
    county: "Contra Costa",
  },
  {
    name: "Ashby",
    code: "ASHB",
    county: "Alameda",
  },
  {
    name: "Balboa Park",
    code: "BALB",
    county: "San Francisco",
  },
  {
    name: "Bay Fair",
    code: "BAYF",
    county: "Alameda",
  },
  {
    name: "Berryessa/North San Jose",
    code: "BERY",
    county: "Santa Clara",
  },
  {
    name: "Castro Valley",
    code: "CAST",
    county: "Alameda",
  },
  {
    name: "Civic Center/UN Plaza",
    code: "CIVC",
    county: "San Francisco",
  },
  {
    name: "Coliseum",
    code: "COLS",
    county: "Alameda",
  },
  {
    name: "Colma",
    code: "COLM",
    county: "San Mateo",
  },
  {
    name: "Concord",
    code: "CONC",
    county: "Contra Costa",
  },
  {
    name: "Daly City",
    code: "DALY",
    county: "San Mateo",
  },
  {
    name: "Downtown Berkeley",
    code: "DBRK",
    county: "Alameda",
  },
  {
    name: "Dublin/Pleasanton",
    code: "DUBL",
    county: "Alameda",
  },
  {
    name: "El Cerrito del Norte",
    code: "DELN",
    county: "Contra Costa",
  },
  {
    name: "El Cerrito Plaza",
    code: "PLZA",
    county: "Contra Costa",
  },
  {
    name: "Embarcadero",
    code: "EMBR",
    county: "San Francisco",
  },
  {
    name: "Fremont",
    code: "FRMT",
    county: "Alameda",
  },
  {
    name: "Fruitvale",
    code: "FTVL",
    county: "Alameda",
  },
  {
    name: "Glen Park",
    code: "GLEN",
    county: "San Francisco",
  },
  {
    name: "Hayward",
    code: "HAYW",
    county: "Alameda",
  },
  {
    name: "Lafayette",
    code: "LAFY",
    county: "Contra Costa",
  },
  {
    name: "Lake Merritt",
    code: "LAKE",
    county: "Alameda",
  },
  {
    name: "MacArthur",
    code: "MCAR",
    county: "Alameda",
  },
  {
    name: "Millbrae",
    code: "MLBR",
    county: "San Mateo",
  },
  {
    name: "Montgomery Street",
    code: "MONT",
    county: "San Francisco",
  },
  {
    name: "North Berkeley",
    code: "NBRK",
    county: "Alameda",
  },
  {
    name: "North Concord/Martinez",
    code: "NCON",
    county: "Contra Costa",
  },
  {
    name: "Oakland International Airport",
    code: "OAKL",
    county: "Alameda",
  },
  {
    name: "Orinda",
    code: "ORIN",
    county: "Contra Costa",
  },
  {
    name: "Pittsburg/Bay Point",
    code: "PITT",
    county: "Contra Costa",
  },
  {
    name: "Pleasant Hill/Contra Costa Centre",
    code: "PHIL",
    county: "Contra Costa",
  },
  {
    name: "Powell Street",
    code: "POWL",
    county: "San Francisco",
  },
  {
    name: "Richmond",
    code: "RICH",
    county: "Contra Costa",
  },
  {
    name: "Rockridge",
    code: "ROCK",
    county: "Alameda",
  },
  {
    name: "San Bruno",
    code: "SBRN",
    county: "San Mateo",
  },
  {
    name: "San Francisco International Airport",
    code: "SFIA",
    county: "San Mateo",
  },
  {
    name: "San Leandro",
    code: "SANL",
    county: "Alameda",
  },
  {
    name: "South Hayward",
    code: "SHAY",
    county: "Alameda",
  },
  {
    name: "South San Francisco",
    code: "SSAN",
    county: "San Mateo",
  },
  {
    name: "Union City",
    code: "UCTY",
    county: "Alameda",
  },
  {
    name: "Walnut Creek",
    code: "WCRK",
    county: "Contra Costa",
  },
  {
    name: "Warm Springs/South Fremont",
    code: "WARM",
    county: "Alameda",
  },
  {
    name: "West Dublin/Pleasanton",
    code: "WDUB",
    county: "Alameda",
  },
  {
    name: "West Oakland",
    code: "WOAK",
    county: "Alameda",
  },
  {
    name: "12th Street/Oakland City Center",
    code: "12TH",
    county: "Alameda",
  },
  {
    name: "16th Street Mission",
    code: "16TH",
    county: "San Francisco",
  },
  {
    name: "19th Street/Oakland",
    code: "19TH",
    county: "Alameda",
  },
  {
    name: "24th Street Mission",
    code: "24TH",
    county: "San Francisco",
  },
  {
    name: "Pittsburg Center",
    code: "PCTR",
    county: "Contra Costa",
  },

  {
    name: "Milpitas Station",
    code: "MLPT",
    county: "Santa Clara",
  },
];

const header = ["date", "hour", "origin", "destination", "exits"];

async function loadFile() {
  const jsonDirectory = path.join(filePath);

  try {
    const data = await fs.readFile(jsonDirectory, { encoding: "utf8" });
    return data;
  } catch (err) {
    console.log(err);
  }
}

async function formatData(data, stats) {
  let promise = new Promise(function (resolve, reject) {
    parse(
      data,
      {
        comment: "#",
        columns: header,
        //skip_records_with_error: true,
        from_line: 1,
        quote: '"',
        // relax_quotes: true,
        relax_column_count: true,
      },
      async function (err, records) {
        if (err) {
          console.log(err);
        }

        console.log(`Records to process: ${records.length}`);

        let obj = {}; // We are going to store the summary here

        console.log("Processing");

        const prepped = records.map((e) => {
          const dateObj = DateTime.fromISO(e.date, {
            zone: "America/Los_Angeles",
          });

          dateObj.plus({ hour: e.hour });
          const dow = _.toNumber(dateObj.toFormat("c"));
          const isWeekend = dow === 6 || dow === 7 ? true : false;
          const origStation = _.filter(stations, function (o) {
            return o.code === e.origin;
          });

          const needle = origStation[0] ? origStation[0] : null;

          const origCounty = needle.county ? needle.county : "INVALID COUNTY";

          // console.log(`processing ${dateObj.toISO()} weekend: ${isWeekend} `);

          const month = dateObj.toFormat("yyyy-MM");

          if (!_.isObjectLike(obj) || !_.isObjectLike(obj[month])) {
            obj[month] = {};
          }

          if (!_.isObjectLike(obj[month][origCounty])) {
            obj[month][origCounty] = {
              weekend: 0,
              weekday_am: 0,
              weekday_pm: 0,
              weekday_offpeak: 0,
            };
          }

          if (isWeekend === true) {
            obj[month][origCounty]["weekend"] =
              obj[month][origCounty]["weekend"] + _.toNumber(e.exits);
          } else {
            // Not a weekend

            if (e.hour >= am_peak_start && e.hour <= am_peak_end) {
              //console.log(`Hour: ${e.hour} - AM PEAK`);
              obj[month][origCounty]["weekday_am"] =
                obj[month][origCounty]["weekday_am"] + _.toNumber(e.exits);
            } else if (e.hour >= pm_peak_start && e.hour <= pm_peak_end) {
              //console.log(`Hour: ${e.hour} - PM PEAK`);
              obj[month][origCounty]["weekday_pm"] =
                obj[month][origCounty]["weekday_pm"] + _.toNumber(e.exits);
            } else {
              //console.log(`Hour: ${e.hour} - OFF PEAK`);
              obj[month][origCounty]["weekday_offpeak"] =
                obj[month][origCounty]["weekday_offpeak"] + _.toNumber(e.exits);
            }
          }
        });
        resolve(obj);
      }
    );
  });
  return promise;
}

const start = async function () {
  const toImport = await loadFile();

  console.log(`File Length of rows to insert: ${toImport.length}`);

  const parsed = await formatData(toImport);

  console.log(parsed)

  const used = process.memoryUsage().heapUsed / 1024 / 1024;
  console.log(
    `The script uses approximately ${Math.round(used * 100) / 100} MB`
  );

  // Don't call process exit right away or it will kill the postgres sessions
};

start();

export default start;
