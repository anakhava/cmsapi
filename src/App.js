import React, { useState } from 'react';
import './App.css';

function App() {
  const [uuid, setUuid] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const handleDownload = async () => {
    if (!uuid.trim()) {
      setError('Please enter a valid UUID');
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const response = await fetch(`https://data.cms.gov/api/1/datastore/query/${uuid}/0`);
      
      if (!response.ok) {
        throw new Error('Failed to fetch data');
      }

      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `cms-data-${uuid}.csv`;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
    } catch (err) {
      setError('Error downloading data: ' + err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="App">
      <header className="App-header">
        <h1>CMS Data Downloader</h1>
      </header>
      <main>
        <div className="container">
          <input
            type="text"
            value={uuid}
            onChange={(e) => setUuid(e.target.value)}
            placeholder="Enter dataset UUID"
            className="uuid-input"
          />
          <button 
            onClick={handleDownload}
            disabled={loading}
            className="download-button"
          >
            {loading ? 'Downloading...' : 'Download Data'}
          </button>
          {error && <div className="error-message">{error}</div>}
        </div>
      </main>
    </div>
  );
}

export default App; 