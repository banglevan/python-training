import React from 'react';
import { Card, CardContent, Typography, Box } from '@mui/material';
import { TrendingUp, TrendingDown } from '@mui/icons-material';

interface MetricCardProps {
    title: string;
    value: number | string;
    change?: number;
    format?: 'number' | 'currency' | 'percent';
    trend?: {
        direction: 'up' | 'down' | 'stable';
        strength: number;
    };
}

export const MetricCard: React.FC<MetricCardProps> = ({
    title,
    value,
    change,
    format = 'number',
    trend
}) => {
    const formatValue = (val: number | string): string => {
        if (typeof val === 'string') return val;
        
        switch (format) {
            case 'currency':
                return new Intl.NumberFormat('en-US', {
                    style: 'currency',
                    currency: 'USD'
                }).format(val);
            
            case 'percent':
                return new Intl.NumberFormat('en-US', {
                    style: 'percent',
                    minimumFractionDigits: 1
                }).format(val);
            
            default:
                return new Intl.NumberFormat('en-US').format(val);
        }
    };

    const getChangeColor = (val?: number): string => {
        if (!val) return 'text.secondary';
        return val > 0 ? 'success.main' : 'error.main';
    };

    return (
        <Card>
            <CardContent>
                <Typography variant="subtitle2" color="text.secondary">
                    {title}
                </Typography>
                
                <Typography variant="h4" component="div" sx={{ my: 1 }}>
                    {formatValue(value)}
                </Typography>
                
                <Box display="flex" alignItems="center" gap={1}>
                    {change !== undefined && (
                        <Typography
                            variant="body2"
                            color={getChangeColor(change)}
                            sx={{ display: 'flex', alignItems: 'center' }}
                        >
                            {change > 0 ? <TrendingUp /> : <TrendingDown />}
                            {change > 0 ? '+' : ''}{(change * 100).toFixed(1)}%
                        </Typography>
                    )}
                    
                    {trend && (
                        <Typography
                            variant="body2"
                            color="text.secondary"
                            sx={{ ml: 'auto' }}
                        >
                            Trend: {trend.direction} ({(trend.strength * 100).toFixed(1)}%)
                        </Typography>
                    )}
                </Box>
            </CardContent>
        </Card>
    );
}; 