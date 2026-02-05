# Health Insurance Membership Dashboard

A comprehensive, portable React dashboard for health insurance membership and enrollment analytics, designed for Databricks Apps.

## 🎯 Features

### Core Analytics
- **Real-time Membership KPIs**: Track total membership, enrollments, terminations, and net change
- **Product Line Analytics**: Visualize membership distribution across HMO, PPO, Medicare Advantage, Medicaid, and ACA products
- **Geographic Analysis**: Regional membership breakdown with risk scoring
- **Demographic Insights**: Age/gender distribution, chronic conditions, and family structure analysis
- **Risk Analytics**: Population risk scoring and distribution
- **Employer Group Tracking**: Monitor top employer groups with tenure and risk metrics
- **YTD Comparisons**: Year-over-year performance tracking

### ✨ New Features
- **📄 PDF Export**: One-click export of any tab to professionally formatted PDF with perfect layout preservation
- **⚡ Progressive Loading**: Visualizations load independently as data becomes ready - no more waiting for everything
- **🔧 Databricks Apps Support**: Full native compatibility with FastAPI backend, automated deployment, and integrated security

## 🏗️ Architecture

### Technology Stack

- **Frontend**: React 18 + TypeScript
- **Build Tool**: Vite
- **Styling**: Tailwind CSS
- **Icons**: Lucide React
- **Data Layer**: Axios + Custom Databricks Service
- **State Management**: React Hooks + Zustand (optional)

### Project Structure

```
standard_member_dashboard/
├── src/
│   ├── components/          # Reusable UI components
│   │   ├── KPICard.tsx
│   │   ├── LoadingSpinner.tsx
│   │   ├── ErrorDisplay.tsx
│   │   └── InsightCard.tsx
│   ├── config/              # Configuration files
│   │   └── app.config.ts
│   ├── hooks/               # Custom React hooks
│   │   ├── useDataQuery.ts
│   │   └── useDashboardData.ts
│   ├── services/            # API/Data services
│   │   └── databricks.service.ts
│   ├── types/               # TypeScript type definitions
│   │   └── index.ts
│   ├── utils/               # Utility functions
│   │   ├── formatters.ts
│   │   └── chartHelpers.ts
│   ├── Dashboard.tsx        # Main dashboard component
│   ├── App.tsx              # Root component
│   ├── main.tsx             # Entry point
│   └── index.css            # Global styles
├── app.config.json          # Databricks app configuration
├── package.json
├── tsconfig.json
├── vite.config.ts
├── tailwind.config.js
└── .env.example
```

## 📊 Data Sources

The dashboard connects to the following Databricks tables/views:

### Views (Semantic Layer)
- `v_membership_kpis` - Monthly membership metrics
- `v_product_mix` - Product line distribution
- `v_age_distribution` - Age band breakdown
- `v_region_summary` - Regional membership and risk
- `v_age_gender_risk` - Demographic risk analysis
- `v_chronic_conditions` - Chronic condition prevalence

### Tables (Direct Queries)
- `dim_members` - Member dimension table
- `fact_membership_monthly` - Monthly membership facts
- `claims_sample_synthetic` - Claims data (optional)

## 🚀 Getting Started

### Prerequisites

- Node.js >= 18.0.0
- npm >= 9.0.0
- Access to a Databricks workspace
- SQL warehouse with appropriate permissions

### Installation

1. **Clone the repository**
   ```bash
   cd /path/to/your/workspace
   git clone <your-repo>
   cd standard_member_dashboard
   ```

2. **Install dependencies**
   ```bash
   npm install
   ```

3. **Configure environment variables**
   ```bash
   cp .env.example .env
   ```

4. **Edit `.env` file with your Databricks credentials**
   ```env
   VITE_DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
   VITE_DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
   VITE_DATABRICKS_TOKEN=your-access-token
   VITE_CATALOG_NAME=your_catalog
   VITE_SCHEMA_NAME=your_schema
   ```

5. **Start development server**
   ```bash
   npm run dev
   ```

6. **Build for production**
   ```bash
   npm run build
   ```

## 🔧 Configuration for Different Organizations

The dashboard is designed to be portable. To adapt it for a different organization:

### Method 1: Environment Variables (Recommended)

Update the `.env` file:

```env
# Change catalog and schema for your organization
VITE_CATALOG_NAME=acme_corp_catalog
VITE_SCHEMA_NAME=health_data
```

### Method 2: Configuration File

Edit `src/config/app.config.ts`:

```typescript
export const config: AppConfig = {
  database: {
    catalog: 'your_org_catalog',
    schema: 'your_org_schema',
  },
  app: {
    refreshInterval: 15 * 60 * 1000, // Adjust refresh interval
    // ... other settings
  },
};
```

### Method 3: Databricks App Configuration

Edit `app.config.json` for Databricks Apps platform:

```json
{
  "configuration": {
    "catalog": {
      "default": "your_catalog"
    },
    "schema": {
      "default": "your_schema"
    }
  }
}
```

## 🎨 Customization

### Branding

Update colors in `tailwind.config.js`:

```javascript
module.exports = {
  theme: {
    extend: {
      colors: {
        primary: {
          500: '#YOUR_PRIMARY_COLOR',
          600: '#YOUR_PRIMARY_DARK',
        },
      },
    },
  },
};
```

### Metrics & KPIs

Add custom metrics in `src/services/databricks.service.ts`:

```typescript
async getCustomMetric(): Promise<any[]> {
  const sql = `
    SELECT ...
    FROM ${getTableName('your_table')}
  `;
  return this.executeQuery(sql);
}
```

## 📈 Performance Optimization

### Caching

The dashboard includes built-in query caching:

```typescript
// Configure in src/config/app.config.ts
app: {
  enableCaching: true,
  cacheTimeout: 5 * 60 * 1000, // 5 minutes
}
```

### Data Refresh

Auto-refresh is configurable:

```typescript
app: {
  refreshInterval: 15 * 60 * 1000, // 15 minutes
}
```

### Build Optimization

The Vite config includes code splitting:

```typescript
rollupOptions: {
  output: {
    manualChunks: {
      vendor: ['react', 'react-dom'],
      icons: ['lucide-react'],
    },
  },
}
```

## 🔐 Security

### Environment Variables

- Never commit `.env` files
- Use Databricks secrets for production deployments
- Rotate access tokens regularly

### API Security

The service layer includes:
- Request timeout (30 seconds)
- Error handling and retry logic
- Query parameter validation

## 🧪 Development

### Type Checking

```bash
npm run type-check
```

### Linting

```bash
npm run lint
```

### Build

```bash
npm run build
```

### Preview Production Build

```bash
npm run preview
```

## 📦 Deployment

### Databricks Apps

1. Build the application:
   ```bash
   npm run build
   ```

2. Deploy to Databricks:
   ```bash
   databricks apps create \
     --name health-insurance-dashboard \
     --source-path ./dist
   ```

3. Configure app settings in Databricks UI

### Static Hosting

The built application (in `dist/`) can be deployed to:
- AWS S3 + CloudFront
- Azure Static Web Apps
- Google Cloud Storage
- Netlify / Vercel

## 🐛 Troubleshooting

### Connection Issues

**Problem**: "Databricks query failed"

**Solution**: 
- Verify credentials in `.env`
- Check SQL warehouse is running
- Validate network connectivity

### Missing Data

**Problem**: Empty charts or "No data available"

**Solution**:
- Verify table names match your catalog/schema
- Check SQL warehouse permissions
- Review view definitions

### Performance Issues

**Problem**: Slow loading times

**Solution**:
- Enable caching in config
- Increase cache timeout
- Optimize SQL queries
- Consider data aggregation

## 📚 Additional Resources

- [Databricks Apps Documentation](https://docs.databricks.com/apps/index.html)
- [React Documentation](https://react.dev)
- [Vite Documentation](https://vitejs.dev)
- [Tailwind CSS](https://tailwindcss.com)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

## 👥 Support

For issues or questions:
- Open a GitHub issue
- Contact your Databricks account team
- Consult internal documentation

---

**Built with ❤️ for Health Insurance Analytics**

