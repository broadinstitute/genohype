import React from 'react';
import ReactDOM from 'react-dom/client';
import { Provider } from 'jotai';
import App from './App';
import './index.css';

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <Provider>
      <App />
    </Provider>
  </React.StrictMode>
);
