import React, { useState, useEffect } from 'react';
import { Grid, Paper, Typography, Box } from '@mui/material';
import { DatePicker } from '@mui/x-date-pickers';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend } from 'recharts';
import { SalesMetrics } from './SalesMetrics';
import { CustomerMetrics } from './CustomerMetrics';
import { ProductMetrics } from './ProductMetrics';
import { MetricsService } from '../services/MetricsService';

interface DashboardProps {
    metricsService: MetricsService;
}

export const Dashboard: React.FC<DashboardProps> = ({ metricsService }) => {
    const [startDate, setStartDate] = useState<Date | null>(null);
    const [endDate, setEndDate] = useState<Date | null>(null);
    const [salesData, setSalesData] = useState<any>(null);
    const [customerData, setCustomerData] = useState<any>(null);
    const [productData, setProductData] = useState<any>(null);
    const [loading, setLoading] = useState<boolean>(true);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        loadData();
    }, [startDate, endDate]);

    const loadData = async () => {
        try {
            setLoading(true);
            setError(null);

            // Load metrics
            const [sales, customers, products] = await Promise.all([
                metricsService.getSalesMetrics(startDate, endDate),
                metricsService.getCustomerMetrics(startDate, endDate),
                metricsService.getProductMetrics(startDate, endDate)
            ]);

            setSalesData(sales);
            setCustomerData(customers);
            setProductData(products);

        } catch (err) {
            setError('Failed to load metrics');
            console.error(err);
        } finally {
            setLoading(false);
        }
    };

    return (
        <Box sx={{ flexGrow: 1, p: 3 }}>
            {/* Header */}
            <Grid container spacing={3} alignItems="center" sx={{ mb: 3 }}>
                <Grid item xs={12} md={6}>
                    <Typography variant="h4" component="h1">
                        Analytics Dashboard
                    </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                    <Box display="flex" justifyContent="flex-end" gap={2}>
                        <DatePicker
                            label="Start Date"
                            value={startDate}
                            onChange={setStartDate}
                        />
                        <DatePicker
                            label="End Date"
                            value={endDate}
                            onChange={setEndDate}
                        />
                    </Box>
                </Grid>
            </Grid>

            {/* Error Message */}
            {error && (
                <Paper sx={{ p: 2, mb: 3, bgcolor: 'error.light' }}>
                    <Typography color="error">{error}</Typography>
                </Paper>
            )}

            {/* Loading State */}
            {loading ? (
                <Box display="flex" justifyContent="center" p={3}>
                    <Typography>Loading metrics...</Typography>
                </Box>
            ) : (
                <Grid container spacing={3}>
                    {/* Sales Metrics */}
                    <Grid item xs={12}>
                        <Paper sx={{ p: 2 }}>
                            <SalesMetrics data={salesData} />
                        </Paper>
                    </Grid>

                    {/* Customer Metrics */}
                    <Grid item xs={12} md={6}>
                        <Paper sx={{ p: 2 }}>
                            <CustomerMetrics data={customerData} />
                        </Paper>
                    </Grid>

                    {/* Product Metrics */}
                    <Grid item xs={12} md={6}>
                        <Paper sx={{ p: 2 }}>
                            <ProductMetrics data={productData} />
                        </Paper>
                    </Grid>

                    {/* Trends Chart */}
                    {salesData?.metrics && (
                        <Grid item xs={12}>
                            <Paper sx={{ p: 2 }}>
                                <Typography variant="h6" gutterBottom>
                                    Revenue Trend
                                </Typography>
                                <LineChart
                                    width={800}
                                    height={400}
                                    data={salesData.metrics}
                                    margin={{
                                        top: 5,
                                        right: 30,
                                        left: 20,
                                        bottom: 5,
                                    }}
                                >
                                    <CartesianGrid strokeDasharray="3 3" />
                                    <XAxis dataKey="sale_date" />
                                    <YAxis />
                                    <Tooltip />
                                    <Legend />
                                    <Line
                                        type="monotone"
                                        dataKey="total_revenue"
                                        stroke="#8884d8"
                                        name="Revenue"
                                    />
                                </LineChart>
                            </Paper>
                        </Grid>
                    )}
                </Grid>
            )}
        </Box>
    );
}; 