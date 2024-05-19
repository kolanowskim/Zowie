import { queue } from "async";
import fetch from "node-fetch";
import { createReadStream } from "fs";
import csv from "csv-parser";
import { createObjectCsvWriter as createCsvWriter } from "csv-writer";
import "dotenv/config";

const csvFilePath = "tickets.csv";
const exportTicketStatuses = "statuses_count_export.csv";
const exportAllTickets = "all_tickets_export.csv";
const apiEndpoint = process.env.API_ENDPOINT;
const delayDuration = 5;
const tickets = []; // all tickets
const ticketStatusCounts = {};
let mappedStatusCounts = {};
const ticketIDlist = []; //ids for fetching
let isFirstRow = true; // Do not take first row

// Function for delay
const delay = (duration) =>
  new Promise((resolve) => setTimeout(resolve, duration));

async function fetchTicketStatus(ticketId) {
  try {
    const response = await fetch(`${apiEndpoint}${ticketId}`, {
      headers: {
        "X-API-KEY": process.env.API_KEY,
      },
    });
    if (!response.ok) {
      throw new Error(
        `Error fetching ticket ${ticketId}: ${response.statusText}`
      );
    }
    return response.json();
  } catch (error) {
    console.error(error);
    return null;
  }
}

// Worker function that will process each task separately in the queue
const processTicket = async (task) => {
  try {
    const result = await fetchTicketStatus(task.ticketId);
    if (result) {
      tickets.push(result);
      const { status } = result;
      ticketStatusCounts[status] = (ticketStatusCounts[status] || 0) + 1;
    }
    await delay(task.delay);
  } catch (error) {
    console.error("Failed to process ticket:", task.ticketId, error);
  }
};

// Create a queue and take only by one by one
const ticketQueue = queue(processTicket, 1);

// Adding tickets to queue
function enqueueTicket(ticketId) {
  ticketQueue.push({ ticketId, delay: delayDuration }, (err) => {
    if (err) {
      console.error("Failed to process ticket:", ticketId, err);
    } else {
      console.log("Ticket processed successfully:", ticketId);
    }
  });
}

// CSV Writer setup for status counts
const csvStatusCountsWriter = createCsvWriter({
  path: exportTicketStatuses,
  header: [
    { id: "status", title: "Status" },
    { id: "count", title: "Count" },
  ],
});

// CSV Writer setup for all tickets
const csvAllTicketsWriter = createCsvWriter({
  path: exportAllTickets,
  header: [
    { id: "ticketId", title: "Ticket ID" },
    { id: "status", title: "Status" },
  ],
});

// Prepare data for CSV export
const makeMapping = () => {
  mappedStatusCounts = Object.entries(ticketStatusCounts).map(
    ([status, count]) => ({
      status: status,
      count: count,
    })
  );
};

function exportToCSV(tickets) {
  csvAllTicketsWriter.writeRecords(tickets).then(() => {
    console.log("All tickets exported");
  });

  makeMapping();

  csvStatusCountsWriter.writeRecords(mappedStatusCounts).then(() => {
    console.log("Statuses exported");
  });
}

//Read initial tickets.csv file and take all ids
createReadStream(csvFilePath)
  .pipe(csv(["id"]))
  .on("data", (row) => {
    if (isFirstRow) {
      isFirstRow = false;
    } else ticketIDlist.push(row.id);
  })
  .on("end", () => {
    console.log(ticketIDlist);
    console.log("fetching all tickets");

    for (const id of ticketIDlist) {
      enqueueTicket(id);
    }
  });

// When all tickets are processed, export to CSV
ticketQueue.drain(() => {
  exportToCSV(tickets);
});
