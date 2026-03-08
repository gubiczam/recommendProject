import { createBrowserRouter, Navigate, RouterProvider } from 'react-router-dom'
import { AppContextProvider } from './context/AppContext'
import { AppLayout } from './layouts/AppLayout'
import { AdminDashboardPage } from './pages/AdminDashboardPage'
import { WebshopPage } from './pages/WebshopPage'

const router = createBrowserRouter([
  {
    path: '/',
    element: <AppLayout />,
    children: [
      { index: true, element: <WebshopPage /> },
      { path: 'admin', element: <AdminDashboardPage /> },
      { path: '*', element: <Navigate to="/" replace /> },
    ],
  },
])

function App() {
  return (
    <AppContextProvider>
      <RouterProvider router={router} />
    </AppContextProvider>
  )
}

export default App
