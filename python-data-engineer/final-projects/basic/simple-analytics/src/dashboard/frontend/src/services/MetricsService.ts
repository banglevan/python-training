import axios from 'axios';
import { format } from 'date-fns';

export class MetricsService {
    private baseUrl: string;

    constructor(baseUrl: string = 'http://localhost:8000/api') {
        this.baseUrl = baseUrl;
    }

    private formatDate(date: Date | null): string | undefined {
        return date ? format(date, 'yyyy-MM-dd') : undefined;
    }

    async getSalesMetrics(
        startDate: Date | null,
        endDate: Date | null,
        groupBy?: string
    ) {
        const params = new URLSearchParams();
        
        if (startDate) {
            params.append('start_date', this.formatDate(startDate)!);
        }
        if (endDate) {
            params.append('end_date', this.formatDate(endDate)!);
        }
        if (groupBy) {
            params.append('group_by', groupBy);
        }

        const response = await axios.get(
            `${this.baseUrl}/metrics/sales?${params.toString()}`
        );
        return response.data;
    }

    async getCustomerMetrics(startDate: Date | null, endDate: Date | null) {
        const params = new URLSearchParams();
        
        if (startDate) {
            params.append('start_date', this.formatDate(startDate)!);
        }
        if (endDate) {
            params.append('end_date', this.formatDate(endDate)!);
        }

        const response = await axios.get(
            `${this.baseUrl}/metrics/customers?${params.toString()}`
        );
        return response.data;
    }

    async getProductMetrics(
        startDate: Date | null,
        endDate: Date | null,
        category?: string
    ) {
        const params = new URLSearchParams();
        
        if (startDate) {
            params.append('start_date', this.formatDate(startDate)!);
        }
        if (endDate) {
            params.append('end_date', this.formatDate(endDate)!);
        }
        if (category) {
            params.append('category', category);
        }

        const response = await axios.get(
            `${this.baseUrl}/metrics/products?${params.toString()}`
        );
        return response.data;
    }
} 