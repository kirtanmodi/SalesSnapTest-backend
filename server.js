require("dotenv").config();
const { BlobServiceClient } = require("@azure/storage-blob");
const csvParser = require("csv-parser");
const stream = require("stream");
const fs = require("fs");

const AZURE_STORAGE_CONNECTION_STRING =
  process.env.AZURE_STORAGE_CONNECTION_STRING;
const CONTAINER_NAME = "main";
const BLOB_Sales = "data/lou_malnatis/dw/Snap/Snap_Sales/Snap_Sales.csv";
const BLOB_PAYROLL = `data/lou_malnatis/dw/Snap/Snap_Payroll/Snap_Payroll.csv`;
const BLOB_PRODUCT = `data/lou_malnatis/dw/Snap/Snap_Product/Snap_Product.csv`;
const BLOB_Sales_Weekly = `data/lou_malnatis/dw/Snap/Snap_Sales_Weekly/Snap_Sales_Weekly.csv`;
// Big Commerce Analytics
const BLOB_REVENUE = `data/taste_of_chicago/dw/BigCommercePortal/Overview/revenue_by_hour/revenue_by_hour.csv`;
const BLOB_SALES_KPI = `data/taste_of_chicago/dw/BigCommercePortal/Overview/Sales_revenue_KPI/Sales_revenue_KPI.csv`;
const OUTPUT_FILE = "Snap_Sales.json";

async function readCSVFromBlob() {
  const blobServiceClient = BlobServiceClient.fromConnectionString(
    AZURE_STORAGE_CONNECTION_STRING
  );
  const containerClient = blobServiceClient.getContainerClient(CONTAINER_NAME);
  const blobClient = containerClient.getBlobClient(BLOB_Sales);
  const downloadBlockBlobResponse = await blobClient.download();
  const readableStream = downloadBlockBlobResponse.readableStreamBody;

  const writeStream = fs.createWriteStream(OUTPUT_FILE);

  writeStream.write("[");

  const readStream = stream.Readable.from(readableStream);
  let firstRow = true;
  readStream
    .pipe(csvParser())
    .on("data", (row) => {
      if (!firstRow) {
        writeStream.write(",");
      }
      writeStream.write(JSON.stringify(row));
      firstRow = false;
    })
    .on("end", () => {
      writeStream.write("]");
      writeStream.end();
      console.log(
        "CSV file successfully processed and written to",
        OUTPUT_FILE
      );
    });
}

readCSVFromBlob().catch((err) => console.error(err));
