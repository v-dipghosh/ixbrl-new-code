import { Container, Typography, Button, Box, Grid } from '@mui/material';
import BlobList, { SelectedBlob, DateRefType } from './components/BlobList';
import { useState, useRef } from 'react';

function App() {
  const [selectedBlobs, setSelectedBlobs] = useState<SelectedBlob[]>([]);
  const dateRef = useRef<DateRefType>(null); // ✅ ref to access dates

  // Azure Function URLs
  const azureFunctionUrls = {
    processUploads: '/api/processUploads',
    callAoai: '/api/callAoai'
  };

  // Generic function to call Azure Functions
  const callAzureFunction = async (url: string, requiredContainer: string) => {
    const validBlobs = selectedBlobs.filter(blob => blob.container === requiredContainer);
    if (validBlobs.length === 0) {
      alert(`Please select a file in the ${requiredContainer} container for this function to process`);
      return;
    }

    // Check: Ensure that no files outside the required container are selected
    if (selectedBlobs.some(blob => blob.container !== requiredContainer)) {
      alert(`Please select only files in the ${requiredContainer} container for this function to process`);
      return;
    }

    // ✅ Get selected dates from BlobList via ref
    // const selectedDates = dateRef.current?.getSelectedDates?.() || [];
    // const selectedDates = dateRef.current?.getSelectedDates();
    // const selectedDates = dateRef.current?.getSelectedDates();
    const selectedDates = dateRef.current?.getSelectedDates() ?? {};


    console.log("🟢 Selected Dates from ref:", selectedDates);
    console.log("🟢 Selected Blobs:", selectedBlobs);

    const payload = {
      blobs: selectedBlobs,
      selectedDates
    };

    console.log("📦 Final Payload Being Sent:", payload);

    try {
      
      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      });

      // const contentType = response.headers.get("content-type");

      const responseText = await response.text();
      console.log("Raw backend response:", responseText);

      // let data;
      // if (contentType && contentType.includes("application/json")) {
      //   data = await response.json();
      // } else {
      //   const responseText = await response.text();
      //   console.error("Unexpected response format:", responseText);
      //   throw new Error(`Unexpected response format: ${responseText}`);
      // }

      if (!response.ok) {
        console.error('Azure Function response:', responseText);
        // alert(`Error: ${data.errors?.join('\n') || 'Unknown error'}`);
        alert(`Success! Validation in progress, Please click the Refresh button after few mins`);
      } else {
        console.log('Azure Function response:', responseText);
        alert(`Validation completed successfully!`);
      }
    } catch (error) {
      console.error('Error calling Azure Function:', error);
      alert(`Error: ${error}`);
    }
  };

  return (
    <Container maxWidth={false} disableGutters sx={{ textAlign: 'center', py: 0 }}>
      <Box
        sx={{
          backgroundColor: '#0A1F44',
          color: 'white',
          py: 3,
          px: 2,
          textAlign: 'center',
          boxShadow: 3,
        }}
      >
        <Typography variant="h4" gutterBottom>
          EY IXBRL DOCUMENT REVIEWER TEST
        </Typography>

        <Box display="flex" justifyContent="center" gap={2} marginTop={2}>
          <Button
            variant="contained"
            color="primary"
            onClick={() => callAzureFunction(azureFunctionUrls.processUploads, "bronze")}
            style={{ display: 'none' }}
          >
            Extract Text
          </Button>

          <Button
            variant="contained"
            color="secondary"
            onClick={() => callAzureFunction(azureFunctionUrls.callAoai, "silver")}
          >
            Call AOAI
          </Button>
        </Box>
      </Box>

      <Grid container spacing={2}>
        <Grid item xs={12}>
          <BlobList
            onSelectionChange={setSelectedBlobs}
            ref={dateRef} // ✅ Pass date ref to BlobList
          />
        </Grid>
      </Grid>
    </Container>
  );
}

export default App;