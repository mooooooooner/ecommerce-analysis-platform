document.addEventListener('DOMContentLoaded', function() {
    const API_BASE_URL = 'http://localhost:8080/api/data';

    // 初始化所有图表
    initSalesChart(API_BASE_URL);
    initProvinceCharts(API_BASE_URL);
    initGenderChart(API_BASE_URL);
    initBrandChart(API_BASE_URL);
    
    // --- 调用新增的图表初始化函数 ---
    initAgeChart(API_BASE_URL);
    initCategoryChart(API_BASE_URL);
});

/**
 * 每日销售额趋势图
 */
async function initSalesChart(baseUrl) {
    const chart = echarts.init(document.getElementById('sales-chart'));
    chart.showLoading();

    try {
        const response = await fetch(`${baseUrl}/sales`);
        const data = await response.json();

        const dates = data.map(item => item.date);
        const values = data.map(item => item.totalSales);

        const option = {
            tooltip: { trigger: 'axis', formatter: '{b}<br/>销售额: ¥ {c}' },
            xAxis: { type: 'category', boundaryGap: false, data: dates },
            yAxis: { type: 'value', axisLabel: { formatter: '¥ {value}' } },
            grid: { left: '3%', right: '4%', bottom: '3%', containLabel: true },
            series: [{
                name: '销售额', type: 'line', smooth: true, areaStyle: {},
                data: values, itemStyle: { color: '#0052cc' }
            }]
        };
        chart.setOption(option);
    } catch (error) {
        console.error('Failed to load sales data:', error);
    } finally {
        chart.hideLoading();
    }
}

/**
 * 省份分布图（地图和柱状图）
 */
async function initProvinceCharts(baseUrl) {
    const mapChart = echarts.init(document.getElementById('province-map-chart'));
    const barChart = echarts.init(document.getElementById('province-bar-chart'));
    mapChart.showLoading();
    barChart.showLoading();

    try {
        const response = await fetch(`${baseUrl}/province`);
        const data = await response.json();
        
        const mapData = data.map(item => ({ name: item.province, value: item.count }));
        const top10Data = data.slice(0, 10).reverse();
        const topProvinces = top10Data.map(item => item.province);
        const topCounts = top10Data.map(item => item.count);
        const maxCount = data.length > 0 ? Math.max(...data.map(d => d.count)) : 0;

        const mapOption = {
            tooltip: { trigger: 'item', formatter: '{b}: {c} 人' },
            visualMap: {
                min: 0, max: maxCount, left: 'left', top: 'bottom',
                text: ['高', '低'], calculable: true,
                inRange: { color: ['#e0f3ff', '#0052cc'] }
            },
            series: [{
                name: '用户省份分布', type: 'map', map: 'china',
                roam: true, label: { show: false }, data: mapData
            }]
        };

        const barOption = {
            tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
            xAxis: { type: 'value', boundaryGap: [0, 0.01] },
            yAxis: { type: 'category', data: topProvinces },
            grid: { left: '3%', right: '4%', bottom: '3%', containLabel: true },
            series: [{
                name: '用户数', type: 'bar', data: topCounts,
                itemStyle: { color: '#0052cc' }
            }]
        };

        mapChart.setOption(mapOption);
        barChart.setOption(barOption);

    } catch (error) {
        console.error('Failed to load province data:', error);
    } finally {
        mapChart.hideLoading();
        barChart.hideLoading();
    }
}

/**
 * 性别分布图
 */
async function initGenderChart(baseUrl) {
    const chart = echarts.init(document.getElementById('gender-chart'));
    chart.showLoading();
    try {
        const response = await fetch(`${baseUrl}/gender`);
        const data = await response.json();

        const option = {
            tooltip: { trigger: 'item', formatter: '{a} <br/>{b}: {c} ({d}%)' },
            legend: { top: 'bottom' },
            series: [{
                name: '性别分布', type: 'pie', radius: '55%',
                data: data,
                emphasis: {
                    itemStyle: { shadowBlur: 10, shadowOffsetX: 0, shadowColor: 'rgba(0, 0, 0, 0.5)' }
                }
            }]
        };
        chart.setOption(option);
    } catch (error) {
        console.error('Failed to load gender data:', error);
    } finally {
        chart.hideLoading();
    }
}

/**
 * 品牌销售图
 */
async function initBrandChart(baseUrl) {
    const chart = echarts.init(document.getElementById('brand-chart'));
    chart.showLoading();
    try {
        const response = await fetch(`${baseUrl}/brand`);
        const data = await response.json();

        const option = {
            tooltip: { trigger: 'item', formatter: '{b}<br/>销售额: ¥{c}' },
            series: [{
                type: 'treemap',
                roam: false, // 禁止拖拽
                nodeClick: false, // 点击无反应
                breadcrumb: { show: false }, // 隐藏面包屑导航
                data: data
            }]
        };
        chart.setOption(option);
    } catch (error) {
        console.error('Failed to load brand data:', error);
    } finally {
        chart.hideLoading();
    }
}

// --- 以下为新增的图表函数 ---

/**
 * 年龄段分布图 (饼图)
 */
async function initAgeChart(baseUrl) {
    const chart = echarts.init(document.getElementById('age-chart'));
    chart.showLoading();
    try {
        const response = await fetch(`${baseUrl}/age`);
        const data = await response.json();

        const option = {
            tooltip: { trigger: 'item', formatter: '{a} <br/>{b}: {c}人 ({d}%)' },
            legend: {
                orient: 'vertical',
                left: 'left',
                top: 'center'
            },
            series: [{
                name: '年龄段分布',
                type: 'pie',
                radius: ['40%', '70%'], // 制作成环形图
                avoidLabelOverlap: false,
                label: {
                    show: false,
                    position: 'center'
                },
                emphasis: {
                    label: {
                        show: true,
                        fontSize: '20',
                        fontWeight: 'bold'
                    }
                },
                labelLine: {
                    show: false
                },
                data: data
            }]
        };
        chart.setOption(option);
    } catch (error) {
        console.error('Failed to load age data:', error);
    } finally {
        chart.hideLoading();
    }
}

/**
 * 热门商品类别销售额 (条形图)
 */
async function initCategoryChart(baseUrl) {
    const chart = echarts.init(document.getElementById('category-chart'));
    chart.showLoading();
    try {
        const response = await fetch(`${baseUrl}/category`);
        const data = await response.json();

        // 按销售额降序排序（从高到低）
        const sortedData = data.sort((a, b) => b.value - a.value);

        // 获取Top 10数据
        const top10Data = sortedData.slice(0, 10);

        // 打印Top 10信息到控制台
        console.log('===== 销售额Top 10类别 =====');
        top10Data.forEach((item, index) => {
            console.log(`${index + 1}. ${item.name}: ¥${item.value}`);
        });

        // 为了在图表中从上到下按销售额降序显示，对Top 10数据进行升序排序
        const chartData = [...top10Data].sort((a, b) => a.value - b.value);

        const categories = chartData.map(item => item.name);
        const sales = chartData.map(item => item.value);

        const option = {
            tooltip: {
                trigger: 'axis',
                axisPointer: { type: 'shadow' },
                formatter: '{b}<br/>销售额: ¥{c}'
            },
            xAxis: {
                type: 'value',
                axisLabel: {
                    formatter: '¥{value}'
                }
            },
            yAxis: { type: 'category', data: categories },
            grid: { left: '3%', right: '4%', bottom: '3%', containLabel: true },
            series: [{
                name: '销售额',
                type: 'bar',
                data: sales,
                itemStyle: {
                    color: '#ff9900'
                }
            }]
        };
        chart.setOption(option);
    } catch (error) {
        console.error('Failed to load category data:', error);
    } finally {
        chart.hideLoading();
    }
}