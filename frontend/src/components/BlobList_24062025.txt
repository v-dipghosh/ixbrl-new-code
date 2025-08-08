import React, { useEffect, useState } from 'react';
import { Button, Card, CardContent, Typography, Box, List, ListItem, ListItemText, Link, Checkbox } from '@mui/material';

const CONTAINER_NAMES = ['silver', 'gold'];

const CONTAINER_LABELS: Record<string, string> = {
  silver: 'Input',
  gold: 'Output',
};

// const baseFunctionUrl = process.env.REACT_APP_FUNCTION_URL;
// console.log("baseFunctionUrl", baseFunctionUrl)

const functionUrl = `/api/app_getBlobsByContainer`;

interface BlobItem {
  name: string;
  url: string;
}

export interface SelectedBlob extends BlobItem {
  container: string;
}

interface BlobListProps {
  onSelectionChange?: (selected: SelectedBlob[]) => void;
}


const BlobList: React.FC<BlobListProps> = ( {onSelectionChange}) => {
  const [blobsByContainer, setBlobsByContainer] = useState<Record<string, BlobItem[]>>({
    //bronze: [],
    silver: [],
    gold: [],
  });

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [selectedBlobs, setSelectedBlobs] = useState<SelectedBlob[]>([]);
  
  const fetchBlobsFromAllContainers = async () => {
    setLoading(true);
    setError(null);

    try {
      const response = await fetch(functionUrl); // Adjust API URL if needed
      console.log("response", response)

      if (!response.ok) {
        throw new Error(`Error: ${response.status} - ${response.statusText}`);
      }

      const data: Record<string, BlobItem[]> = await response.json();
      setBlobsByContainer(data);
    } catch (err: unknown) {
      if (err instanceof Error) {
        setError(`Error: ${err.message || 'Unknown error'}`);
      }
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchBlobsFromAllContainers();
  }, []);


  // Toggle selection for a blob file
  const toggleSelection = (container: string, blob: BlobItem) => {
    const exists = selectedBlobs.some(
      (b) => b.name === blob.name && b.container === container
    );
    let newSelection: SelectedBlob[];
    if (exists) {
      newSelection = selectedBlobs.filter(
        (b) => !(b.name === blob.name && b.container === container)
      );
    } else {
      newSelection = [...selectedBlobs, { ...blob, container }];
    }
    setSelectedBlobs(newSelection);
    
    if (onSelectionChange) {
      onSelectionChange(newSelection);
    }
    };

    //Handle file upload
    const handleFileUpload = async (
        event: React.ChangeEvent<HTMLInputElement>,
        containerName: string
    ) => {
        const file = event.target.files?.[0];
        if (!file) return;

        const formData = new FormData();
        formData.append("file", file); //file input
        formData.append("containerName", containerName); //container name in which the file to be uploaded

        try {
            const response = await fetch("/api/app_uploadBlob", {
                method: "POST",
                body: formData,
            });

            const result = await response.json();

            if (response.ok) {
                alert("Upload successful!");
                fetchBlobsFromAllContainers(); // Refresh the blob list
            } else {
                alert(`Upload failed: ${result.message || "Unknown error"}`);
            }
        } catch (error) {
            console.error("Upload error:", error);
            alert("Upload failed.");
        }
    };

    //Handle Single Or Multiple file download
    const handleDownloadSelected = async (container: string) => {
        const filesToDownload = selectedBlobs.filter(b => b.container === container);

        for (const blob of filesToDownload) {
            try {
                const response = await fetch(`/api/app_downloadBlobs?containerName=${blob.container}&blobName=${encodeURIComponent(blob.name)}`);
                if (!response.ok) throw new Error(`Download failed for ${blob.name}`);

                const blobData = await response.blob();
                const url = window.URL.createObjectURL(blobData);

                const link = document.createElement("a");
                link.href = url;
                link.setAttribute("download", blob.name);
                document.body.appendChild(link);
                link.click();
                link.remove();
            } catch (error) {
                console.error(`Error downloading ${blob.name}:`, error);
                alert(`Error downloading ${blob.name}`);
            }
        }
    };


  return (
    <div style={{ padding: '1rem', border: '1px solid #ddd', borderRadius: '4px' }}>
      <Typography variant="h5" gutterBottom>
        Blob Viewer
      </Typography>
      <Box marginBottom={2}>
        <Button variant="contained" color="secondary" onClick={fetchBlobsFromAllContainers} disabled={loading}>
          {loading ? 'Refreshing...' : 'Refresh'}
        </Button>
      </Box>

      {error && (
        <Typography variant="body1" color="error" gutterBottom>
          {error}
        </Typography>
      )}

    {CONTAINER_NAMES.map((containerName) => {
      const blobItems = blobsByContainer[containerName] || [];
      return (
        <Card key={containerName} sx={{ marginBottom: 2 }}>
          <CardContent>
            <Box display="flex" justifyContent="space-between" alignItems="center">
              <Typography variant="h6" gutterBottom>
              Container: {CONTAINER_LABELS[containerName] || containerName}
              </Typography>

              {containerName === 'silver' && (
                <>
                 <input
                   type="file"
                    id={`file-input-${containerName}`}
                    style={{ display: 'none' }}
                    onChange={(e) => handleFileUpload(e, containerName)}
                 />
                 <label htmlFor={`file-input-${containerName}`}>
                   <Button variant="outlined" color="primary" size="small" component="span">
                     Upload
                   </Button>
                 </label>
                 </>
               )}
                {containerName === 'gold' && (
                  <Button
                    variant="outlined"
                    color="success"
                    size="small"
                    onClick={() => handleDownloadSelected(containerName)}
                    disabled={selectedBlobs.filter(b => b.container === containerName).length === 0}
                  >
                  Download
                  </Button>
                )}
            </Box>

            {blobItems.length === 0 ? (
              <Typography variant="body2">No files present</Typography>
            ) : (
              <List dense>
                {blobItems.map((blob) => (
                  <ListItem key={blob.name} disablePadding>
                    <Checkbox
                      checked={selectedBlobs.some(
                        (b) => b.name === blob.name && b.container === containerName
                      )}
                      onChange={() => toggleSelection(containerName, blob)}
                    />
                    <ListItemText
                      primary={
                        <Link href={blob.url} target="_blank" rel="noopener noreferrer">
                          {blob.name}
                        </Link>
                      }
                      primaryTypographyProps={{ align: 'center' }}
                    />
                  </ListItem>
                ))}
              </List>
            )}
          </CardContent>
        </Card>
      );
    })}
    </div>
  );
};

export default BlobList;
