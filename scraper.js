require("dotenv").config();
const { Webhook, MessageBuilder } = require("discord-webhook-node");
const fs = require("fs");

// Import MongoDB Driver
const { MongoClient } = require("mongodb");
//const ObjectId = require('mongodb').ObjectID;
const mongoDbUri = process.env.MONGODB_CON_STRING;
const { ObjectId } = require("mongodb");

// Function to connect to MongoDB
const connectToMongoDB = async () => {
  try {
    const mongoDbUri = process.env.MONGODB_CON_STRING;
    const client = new MongoClient(mongoDbUri, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
    });
    await client.connect();
    console.log("Connected to MongoDB successfully.");
    return client;
  } catch (error) {
    console.error("Error while connecting to MongoDB:", error);
    throw error;
  }
};

let discrepanciesCollection;

// Add stealth plugin and use defaults
const pluginStealth = require("puppeteer-extra-plugin-stealth");
const { executablePath } = require("puppeteer");

const axios = require("axios");
const puppeteer = require("puppeteer-extra");
const webhook = new Webhook(process.env.WEBHOOK_URL);
const sentProjections = new Map();

const extractFirstTwoWords = (str) => {
  const words = str.trim().split(" ");
  return words.slice(0, 2).join(" ");
};

const leagueConfigurations = [
  {
    id: "12", // MMA
    statTypes: {
      "Significant Strikes": 5,
      Takedowns: 0.5,
    },
  },
  {
    id: "1", // PGA
    statTypes: {
      Strokes: 0.5,
    },
  },
  {
    id: "144", // TBT
    statTypes: {
      Points: 2,
    },
  },
  {
    id: "5", // TENNIS
    statTypes: {
      Aces: 1,
    },
  },
  {
    id: "82", // SOCCER
    statTypes: {
      Shots: 0.5,
      "Goalie Saves": 0.5,
      "Passes Attempted": 5,
      Clearances: 1,
      Tackles: 1,
    },
  },
  {
    id: "3", // WNBA
    statTypes: {
      Points: 2,
      Rebounds: 1,
      Assists: 1,
      "Pts+Rebs+Asts": 3,
      "Rebs+Asts": 1,
      "3-PT Made": 1,
      "Free Throws Made": 1,
      "Pts+Asts": 1,
    },
  },
  {
    id: "2", // MLB
    statTypes: {
      "Pitcher Strikeouts": 0.5,
      "Pitches Thrown": 5,
      "Total Bases": 0.5,
      "Walks Allowed": 0.5,
      "Hits+Runs+RBIS": 0.5,
      "Hitter Strikeouts": 0.5,
      "Hits Allowed": 0.5,
      "Earned Runs Allowed": 0.5,
    },
  },
  {
    id: "135", // KBO
    statTypes: {
      "Pitcher Strikeouts": 0.5,
      "Pitches Thrown": 0.5,
      "Total Bases": 0.5,
      "Walks Allowed": 0.5,
      "Hits+Runs+RBIS": 0.5,
      "Hits Allowed": 0.5,
    },
  },
  {
    id: "163", // NFLSZN
    statTypes: {
      "Pass TDs": 1,
      "Rush TDs": 1,
      "Rush Yards": 75,
      "Pass Yards": 75,
      "Receiving Yards": 75,
      "Rec TDs": 1,
      INT: 1,
      "Pass INTs": 1,
    },
  },
  {
    id: "9", // NFL
    statTypes: {
      "Pass TDs": 0.5,
      "Rush TDs": 0.5,
      "Rush Yards": 10,
      "Pass Yards": 10,
      "Receiving Yards": 10,
      "Rec TDs": 0.5,
      INT: 0.5,
      "Pass INTs": 0.5,
    },
  },
  {
    id: "11", // CFL
    statTypes: {
      "Pass TDs": 0.5,
      "Rush TDs": 0.5,
      "Rush Yards": 10,
      "Pass Yards": 10,
      "Receiving Yards": 10,
      "Rec TDs": 0.5,
      INT: 0.5,
      "Pass INTs": 0.5,
    },
  },
  {
    id: "4", // NASCAR
    statTypes: {
      "Laps Led": 0.5,
    },
  },
  {
    id: "121", // LoL
    statTypes: {
      "MAPS 1-2 Kills": 1,
      "MAPS 1-3 Kills": 2,
      "MAP 1 Kills": 1,
      "MAP 3 Kills": 0.5,
      "MAP 4 Kills": 0.5,
      "MAP 5 Kills": 0.5,
    },
  },
  {
    id: "159", // VAL
    statTypes: {
      "MAPS 1-2 Kills": 3,
      "MAPS 1-3 Kills": 3,
      "MAP 1 Kills": 1,
      "MAP 3 Kills": 1,
      "MAP 4 Kills": 1,
      "MAP 5 Kills": 1,
    },
  },
  {
    id: "124", // CSGO
    statTypes: {
      "MAPS 1-2 Kills": 3,
      "MAPS 1-2 Headshots": 2,
      "MAP 3 Kills": 1,
      "MAP 3 Headshots": 1,
    },
  },
  /*
{
  id: '174', // DOTA
  statTypes: {
	'MAPS 1-2 Kills': 3, // NEED TO MAP TO DISPLAY STAT: 'Kills in Game 1+2'
  },
},*/
];

// Mapping of stat type to display stat
const statTypeToDisplayStat = {
  Shots: "Shots Attempted",
  "Rebs+Asts": "Rebounds + Assists",
  "Pts+Rebs+Asts": "Pts + Rebs + Asts",
  "3-PT Made": "3-Pointers Made",
  "Pts+Asts": "Points + Assists",
  "Pitcher Strikeouts": "Strikeouts",
  "Pitches Thrown": "Pitch Count",
  "Hits+Runs+RBIS": "Hits + Runs + RBIs",
  "Hitter Strikeouts": "Batter Strikeouts",
  "Goalie Saves": "Saves",
  "Passes Attempted": "Passes",
  "Pass TDs": "Passing TDs",
  "Rush TDs": "Rushing TDs",
  "Rush Yards": "Rushing Yards",
  "Pass Yards": "Passing Yards",
  "Rec TDs": "Receiving TDs",
  INT: "Interceptions",
  "Pass INTs": "Interceptions",
  "MAPS 1-2 Kills": "Kills on Map 1+2",
  "MAPS 1-3 Kills": "Kills on Maps 1+2+3",
  "MAP 1 Kills": "Kills on Map 1",
  "MAP 3 Kills": "Kills on Map 3",
  "MAP 4 Kills": "Kills on Map 4",
  "MAP 5 Kills": "Kills on Map 5",
  "MAPS 1-2 Headshots": "Headshots on Maps 1+2",
  "MAP 3 Headshots": "Headshots on Map 3",
};

let projectionCounter = 1; // Initialize the unique ID counter

const convertDateToUTC = (dateString) => {
  const date = new Date(dateString);
  return Date.UTC(
    date.getUTCFullYear(),
    date.getUTCMonth(),
    date.getUTCDate(),
    date.getUTCHours(),
    date.getUTCMinutes(),
    date.getUTCSeconds()
  );
};

// Function to log discrepancies to MongoDB
const logDiscrepanciesToMongoDB = async (
  discrepancies,
  discrepanciesCollection
) => {
  // Check if there are any discrepancies to log
  if (discrepancies.length === 0) {
    console.log("No discrepancies to log.");
    return; // Exit the function as there are no discrepancies to log
  }

  try {
    // Insert the discrepancies into the collection
    const insertResult = await discrepanciesCollection.insertMany(
      discrepancies
    );
    console.log(
      `${insertResult.insertedCount} discrepancies inserted into MongoDB.`
    );
  } catch (error) {
    console.error("Error while logging discrepancies to MongoDB:", error);
  }
};

const deleteDiscrepanciesWithNoMatch = async (
  discrepanciesCollection,
  prizePicksData,
  underdogApiData
) => {
  try {
    const discrepancies = await discrepanciesCollection.find({}).toArray();
    const discrepanciesToDelete = [];

    for (const discrepancy of discrepancies) {
      const isDataIdFound = prizePicksData.data.some(
        (item) =>
          item.id === discrepancy.dataId &&
          item.attributes.line_score === discrepancy.lineScore &&
          discrepancy.underdogLineScore &&
          underdogApiData.over_under_lines.some(
            (item) =>
              parseFloat(item.stat_value) ===
              parseFloat(discrepancy.underdogLineScore)
          )
      );
      if (!isDataIdFound) {
        discrepanciesToDelete.push(discrepancy._id);
      }
    }

    if (discrepanciesToDelete.length > 0) {
      await discrepanciesCollection.deleteMany({
        _id: { $in: discrepanciesToDelete.map((id) => new ObjectId(id)) },
      });
      console.log(
        `${discrepanciesToDelete.length} discrepancies deleted from MongoDB.`
      );
    } else {
      console.log("No discrepancies to delete.");
    }
  } catch (error) {
    console.error("Error while handling discrepancies:", error);
  }
};

const storeProjectionsToMongoDB = async (
  projections,
  projectionsCollection
) => {
  try {
    if (projections.length === 0) {
      console.log("No projections to store.");
      return;
    }

    const existingDataIds =
      (await projectionsCollection.distinct("dataId")) || [];

    const projectionsToInsert = projections.filter((projection) => {
      const dataId = projection.dataId;
      return !existingDataIds.includes(dataId);
    });

    if (projectionsToInsert.length === 0) {
      console.log("All projections already exist in the database.");
      return;
    }

    const insertResult = await projectionsCollection.insertMany(
      projectionsToInsert
    );
    console.log(
      `${insertResult.insertedCount} projections inserted into MongoDB.`
    );
  } catch (error) {
    console.error("Error while storing projections to MongoDB:", error);
    // Handle the error appropriately, such as retrying the operation or logging the error to a monitoring service.
  }
};

const deleteOldProjections = async (projectionsCollection) => {
  try {
    // Calculate the timestamp for 3 days ago
    const threeDaysAgo = new Date();
    threeDaysAgo.setDate(threeDaysAgo.getDate() - 3);

    // Delete projections older than threeDaysAgo
    const deleteResult = await projectionsCollection.deleteMany({
      timestamp: { $lt: threeDaysAgo.toISOString() },
    });

    console.log(`${deleteResult.deletedCount} old projections deleted.`);
  } catch (error) {
    console.error("Error while deleting old projections:", error);
  }
};

const runScript = async () => {
  try {
    // Connect to MongoDB
    const client = await connectToMongoDB();
    const db = client.db("datawise");
    const discrepanciesCollection = db.collection("discrepancies");
    // Fetch existing discrepancies from MongoDB
    const existingDiscrepancies = await discrepanciesCollection
      .find({})
      .toArray();
    // console.log(existingDiscrepancies);   // discrepancies in the DataBase Collection
    const projectionsCollection = db.collection("projections");

    let underdogApiData;
    try {
      const response = await axios.get(process.env.UNDERDOG_API_URL);

      // Assuming the API returns an array of data
      const data = response.data;
      console.log("Underdog data fetched successfully:");
      // console.log(data);
      underdogApiData = data;
    } catch (error) {
      console.error("Error while fetching data:", error.message);
    }

    let prizePicksData;
    try {
      puppeteer.use(pluginStealth());
      await puppeteer
        .launch({
          executablePath:
            "C:/Program Files/Google/Chrome/Application/chrome.exe",
          headless: "new", // For new headless mode
        })
        .then(async (browser) => {
          const page = await browser.newPage();
          await page.setViewport({
            width: 1280,
            height: 720,
          });
          await page.goto(process.env.PRIZEPICKS_API_URL);
          await page.waitForTimeout(1000);
          await page.content();

          const innerText = await page.evaluate(() => {
            return JSON.parse(document.querySelector("body").innerText);
          });
          await browser.close();
          prizePicksData = innerText;
          console.log("Prize Picks data fetched successfully:");
          // console.log(prizePicksData);
        });
    } catch (error) {
      console.error("Error while fetching data:", error.message);
    }

    const prizePicksProjections = prizePicksData.data; // this is PrizePickData Data coming from URL
    const prizePicksIncluded = prizePicksData.included; // this is PrizePickData Included data coming from URL
    const underdogProjections = underdogApiData.over_under_lines; // this is underDogData overUnderLines coming from URL
    const currentTimestamp = new Date().toISOString(); // this is current time stamp
    // Store all projections to MongoDB
    const projectionsToStore = prizePicksProjections
      .map((projection) => {
        const dataId = projection.id;
        // console.log(dataId);
        const leagueId = projection.relationships.league.data.id;

        // Check if the included.type is "new_player" before proceeding
        if (projection.relationships?.new_player?.data?.type === "new_player") {
          const includedId = projection.relationships.new_player.data.id; // this is newPlayer id here
          const includedPlayer = prizePicksIncluded.find(
            (player) => player.id === includedId && player.type === "new_player"
          );
          // we compare IDs of the prizePickData Data and included and playeType as well

          // Check if the included player is found before proceeding
          if (includedPlayer) {
            const playerName = includedPlayer.attributes.name;
            const lineScore = projection.attributes.line_score;
            const statType = projection.attributes.stat_type;
            const description = projection.attributes.description;
            return {
              dataId,
              leagueId,
              playerName,
              description,
              lineScore,
              statType,
              timestamp: currentTimestamp,
            };
          } else {
            console.error(`Included player with ID ${includedId} not found.`);
          }
        } else {
          console.error(
            `Projection with ID ${dataId} is not associated with a new_player.`
          );
        }

        return null; // If there's an error or the projection is not associated with a new_player, skip it
      })
      .filter((projection) => projection !== null); // Filter out null values from the map operation

    const relevantProjections = prizePicksProjections.filter(
      ({
        attributes: { stat_type },
        relationships: {
          league: {
            data: { id },
          },
        },
      }) => {
        const leagueConfig = leagueConfigurations.find(
          (config) => config.id === id
        );
        return leagueConfig && stat_type in leagueConfig.statTypes;
      }
    );

    // console.log(relevantProjections);

    const discrepancies = [];
    const discrepanciesToDelete = [];

    // Process valid discrepancies and update as needed
    for (const existingDiscrepancy of existingDiscrepancies) {
      const { uniqueId, leagueId, playerName, statType, dataId } =
        existingDiscrepancy; // this is the Discrepancies which is in MongoDb
      const matchingDiscrepancy = relevantProjections.find(
        (projection) =>
          projection.uniqueId === uniqueId &&
          projection.leagueId === leagueId &&
          projection.prizePicksProjection.attributes.stat_type === statType &&
          projection.prizePicksProjection.id === dataId
      );

      // console.log(matchingDiscrepancy);

      if (!matchingDiscrepancy) {
        // The existing discrepancy no longer exists in the retrieved data, delete it
        discrepanciesToDelete.push(existingDiscrepancy);
      } else {
        const lineScore =
          matchingDiscrepancy.prizePicksProjection.attributes.line_score;
        const underdogLineScore = parseFloat(
          matchingDiscrepancy.underdogProjection.stat_value
        );
        const variance = Math.abs(lineScore - underdogLineScore);
        const leagueConfig = leagueConfigurations.find(
          (config) => config.id === leagueId
        );
        const varianceThreshold = leagueConfig.statTypes[statType];

        if (variance >= varianceThreshold) {
          // The existing discrepancy is still valid, no need to update the lineScore
          if (existingDiscrepancy.variance !== variance) {
            // Variance has changed, delete the discrepancy and let the script automatically make a new record
            discrepanciesToDelete.push(existingDiscrepancy);
          } else {
            // Variance has not changed, push the updated discrepancy to the new discrepancies array
            existingDiscrepancy.lineScore = lineScore;
            existingDiscrepancy.underdogLineScore = underdogLineScore;
            existingDiscrepancy.variance = variance;
            discrepancies.push(existingDiscrepancy);
          }
        } else {
          // The discrepancy no longer meets the variance criteria, delete it
          discrepanciesToDelete.push(existingDiscrepancy);
        }
      }
    }

    const projectionsToSend = relevantProjections.reduce(
      (accumulator, projection) => {
        const includedId = projection.relationships.new_player.data.id;
        const includedPlayer = prizePicksIncluded.find(
          (player) => player.id === includedId
        );
        const playerName = includedPlayer.attributes.name;
        const statType = projection.attributes.stat_type;
        const lineScore = projection.attributes.line_score;
        const leagueId = projection.relationships.league.data.id;
        const formattedPlayerName =
          leagueId === "121" ? playerName : extractFirstTwoWords(playerName); // ESPORTS Player Matching

        let underdogProjection = underdogProjections.find(
          ({
            over_under: {
              title,
              appearance_stat: { display_stat },
            },
          }) =>
            new RegExp(`\\b${formattedPlayerName}\\b`, "i").test(title) &&
            display_stat === (statTypeToDisplayStat[statType] || statType)
        );
        if (["121", "159", "124"].includes(leagueId)) {
          // include '174' here for Dota when it is functional
          // Find the matching player in Underdog data based on last name
          const underdogPlayer = underdogApiData.players.find(
            (player) =>
              player.last_name.toLowerCase() ===
              formattedPlayerName.toLowerCase()
          );
          if (underdogPlayer) {
            // Find the game that matches the player's team id
            const underdogGame = underdogApiData.games.find(
              (game) =>
                game.home_team_id === underdogPlayer.team_id ||
                game.away_team_id === underdogPlayer.team_id
            );

            if (underdogGame) {
              // Convert both times to UTC for comparison
              const prizePicksStartTimeUTC = convertDateToUTC(
                projection.attributes.start_time
              );
              const underdogStartTimeUTC = convertDateToUTC(
                underdogGame.scheduled_at
              );

              // Compare the start times
              if (
                Math.abs(prizePicksStartTimeUTC - underdogStartTimeUTC) <=
                1000 * 60 * 60
              ) {
                // Adjust this value based on the maximum allowed difference in start times
                // Correct underdogProjection found, no further action required
              } else {
                //console.log(`Game for player ${playerName} does not match start times in PrizePicks and Underdog.`);
                underdogProjection = null;
              }
            } else {
              //console.log(`No game found in Underdog for player ${playerName}.`);
              underdogProjection = null;
            }
          } else {
            //console.log(`Player ${playerName} not found in Underdog data.`);
            underdogProjection = null;
          }
        }

        if (!underdogProjection) {
          //console.log(`No Underdog projections available for ${playerName} with stat type ${statType}`);
          return accumulator;
        }

        const underdogLineScore = parseFloat(underdogProjection.stat_value);
        const variance = Math.abs(lineScore - underdogLineScore);

        const leagueConfig = leagueConfigurations.find(
          (config) => config.id === leagueId
        );
        const varianceThreshold = leagueConfig.statTypes[statType];

        if (variance < varianceThreshold) {
          //console.log(`Player ${playerName} with stat type ${statType} did not meet the variance threshold. Current variance: ${variance}`);
        } else {
          const projectionId = `${leagueId}-${includedId}-${statType}`;
          const prevVariance = sentProjections.get(projectionId);
          if (prevVariance === undefined || prevVariance !== variance) {
            sentProjections.set(projectionId, variance);
            const uniqueId = projectionCounter++; // Generate unique ID
            accumulator.push({
              uniqueId,
              prizePicksProjection: projection,
              underdogProjection,
              isHigherProjection: lineScore > underdogLineScore, // compare the two line scores
              ouIndicator: lineScore > underdogLineScore ? "u" : "o", // define the ouIndicator based on the line scores
              dataId: projection.id,
            });

            // Create discrepancy object and push it to the discrepancies array
            const discrepancy = {
              uniqueId,
              leagueId,
              playerName,
              statType,
              lineScore,
              underdogLineScore,
              variance,
              dataId: projection.id,
            };
            discrepancies.push(discrepancy);
          }
        }

        return accumulator;
      },
      []
    );

    console.log(`Number of Discrepancies Found: ${projectionsToSend.length}`);

    const failedEmbeds = [];
    const successEmbeds = [];

    for (const {
      uniqueId,
      prizePicksProjection,
      underdogProjection,
    } of projectionsToSend) {
      const includedId = prizePicksProjection.relationships.new_player.data.id;
      const includedPlayer = prizePicksIncluded.find(
        (player) => player.id === includedId
      );
      const playerName = includedPlayer.attributes.name;
      const leagueName = includedPlayer.attributes.league;
      const teamName = includedPlayer.attributes.team;
      const position = includedPlayer.attributes.position;
      const playerImage = includedPlayer.attributes.image_url;

      const statType = prizePicksProjection.attributes.stat_type;
      const lineScore = prizePicksProjection.attributes.line_score;
      const description = prizePicksProjection.attributes.description;

      const underdogLineScore = parseFloat(underdogProjection.stat_value);
      const variance = Math.abs(lineScore - underdogLineScore);

      const lowestProjection = Math.min(lineScore, underdogLineScore);
      const highestProjection = Math.max(lineScore, underdogLineScore);

      const lowestProjectionText =
        lowestProjection === lineScore ? "PrizePicks" : "Underdog";
      const highestProjectionText =
        highestProjection === lineScore ? "PrizePicks" : "Underdog";

      const projectionsText = `PrizePicks: ${lineScore} (${
        lineScore === lowestProjection ? "over" : "under"
      })\nUnderdog: ${underdogLineScore} (${
        underdogLineScore === lowestProjection ? "over" : "under"
      })`;

      const startTime = new Date(prizePicksProjection.attributes.start_time);
      const options = {
        weekday: "short",
        month: "short",
        day: "numeric",
        hour: "numeric",
        minute: "2-digit",
        timeZone: "America/New_York",
        timeZoneName: "short",
      };
      const formattedStartTime = startTime
        .toLocaleString("en-US", options)
        .replace("EDT", "EST");

      const embed = new MessageBuilder()
        .setTitle(`${leagueName} - ${playerName}`)
        .setDescription(`*${teamName} - ${position}\nvs ${description}*`)
        .addField(statType, projectionsText.toString(), true)
        .addField("Variance", variance.toString(), true)
        .addField("Start Time", formattedStartTime)
        .addField("ID", uniqueId.toString(), false)
        .setThumbnail(playerImage)
        .setColor("#d7be7a")
        .setFooter("Testing | Version: 1.0.1");

      // must be uncomment
      // try {
      //   let check = await webhook.send(embed);
      //   console.log(
      //     `Webhook sent successfully for ${playerName} with stat type ${statType}, ${check}`
      //   );
      //   successEmbeds.push(JSON.parse(JSON.stringify(embed)));
      // } catch (error) {
      //   console.error(
      //     `Error sending webhook for ${playerName} with stat type ${statType}:`,
      //     error
      //   );
      //   failedEmbeds.push(JSON.parse(JSON.stringify(embed)));
      // }
    }

    // Store all projections found to projections collection
    await storeProjectionsToMongoDB(projectionsToStore, projectionsCollection);

    // Call the functions with the MongoDB client and collection as arguments
    await logDiscrepanciesToMongoDB(discrepancies, discrepanciesCollection);

    // Delete discrepancies that are off the board
    await deleteDiscrepanciesWithNoMatch(
      discrepanciesCollection,
      prizePicksData,
      underdogApiData
    );

    // Delete stored projections that are older than 3 days
    await deleteOldProjections(projectionsCollection);

    // Close the MongoDB Client
    client.close();

    // Wait for next run
    const waitTime = 5 * 60 * 1000; // 5 minutes in milliseconds
    const waitTimeInMinutes = waitTime / 60000; // Convert milliseconds to minutes

    console.log(`Waiting ${waitTimeInMinutes} minutes until the next run...`);
    setTimeout(runScript, waitTime);
  } catch (error) {
    console.error(`Error during run:`, error);
    const waitTime = 5 * 60 * 1000; // 5 minutes in milliseconds
    setTimeout(runScript, waitTime);
  }
};

runScript();
