const API_BASE_URL = 'http://localhost:8080/api/data';
document.addEventListener('DOMContentLoaded', function() {

    // 初始化所有图表
    initSalesChart();
    initProvinceCharts();
    initGenderChart(); // 加载预留的图表
    initBrandChart(); // 加载预留的图表
});

/**
 * 每日销售额趋势图
 */
async function initSalesChart() {
    const chart = echarts.init(document.getElementById('sales-chart'));
    chart.showLoading();

    try {
        const response = await fetch(`${API_BASE_URL}/sales`);
        const data = await response.json();

        const dates = data.map(item => item.date);
        const values = data.map(item => item.totalSales);

        const option = {
            tooltip: {
                trigger: 'axis',
                formatter: '{b}<br/>销售额: ¥ {c}'
            },
            xAxis: {
                type: 'category',
                boundaryGap: false,
                data: dates
            },
            yAxis: {
                type: 'value',
                axisLabel: {
                    formatter: '¥ {value}'
                }
            },
            grid: {
                left: '3%',
                right: '4%',
                bottom: '3%',
                containLabel: true
            },
            series: [{
                name: '销售额',
                type: 'line',
                smooth: true,
                areaStyle: {},
                data: values,
                itemStyle: {
                    color: '#0052cc'
                }
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
async function initProvinceCharts() {
    const mapChart = echarts.init(document.getElementById('province-map-chart'));
    const barChart = echarts.init(document.getElementById('province-bar-chart'));
    mapChart.showLoading();
    barChart.showLoading();

    try {
        const response = await fetch(`${API_BASE_URL}/province`);
        const data = await response.json();
        
        // 准备地图数据
        const mapData = data.map(item => ({ name: item.province, value: item.count }));
        
        // 准备柱状图数据 (Top 10)
        const top10Data = data.slice(0, 10).reverse(); // reverse for horizontal bar
        const topProvinces = top10Data.map(item => item.province);
        const topCounts = top10Data.map(item => item.count);
        
        const maxCount = Math.max(...data.map(d => d.count));

        // 地图配置
        const mapOption = {
            tooltip: {
                trigger: 'item',
                formatter: '{b}: {c} 人'
            },
            visualMap: {
                min: 0,
                max: maxCount,
                left: 'left',
                top: 'bottom',
                text: ['高', '低'],
                calculable: true,
                inRange: {
                    color: ['#e0f3ff', '#0052cc']
                }
            },
            series: [{
                name: '用户省份分布',
                type: 'map',
                map: 'china',
                roam: true,
                label: {
                    show: false
                },
                data: mapData
            }]
        };

        // 柱状图配置
        const barOption = {
            tooltip: {
                trigger: 'axis',
                axisPointer: { type: 'shadow' }
            },
            xAxis: { type: 'value', boundaryGap: [0, 0.01] },
            yAxis: { type: 'category', data: topProvinces },
            grid: { left: '3%', right: '4%', bottom: '3%', containLabel: true },
            series: [{
                name: '用户数',
                type: 'bar',
                data: topCounts,
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
 * 【可扩展】性别分布图
 */
async function initGenderChart() {
    const chart = echarts.init(document.getElementById('gender-chart'));
    chart.showLoading();
    try {
        const response = await fetch(`${API_BASE_URL}/gender`);
        const data = await response.json(); // API返回 [{name: '男性', value: 123}, ...]

        const option = {
            tooltip: { trigger: 'item', formatter: '{a} <br/>{b}: {c} ({d}%)' },
            legend: { top: 'bottom' },
            series: [{
                name: '性别分布',
                type: 'pie',
                radius: '50%',
                data: data,
                emphasis: {
                    itemStyle: {
                        shadowBlur: 10,
                        shadowOffsetX: 0,
                        shadowColor: 'rgba(0, 0, 0, 0.5)'
                    }
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
 * 【可扩展】品牌销售图
 */
async function initBrandChart() {
    const chart = echarts.init(document.getElementById('brand-chart'));
    chart.showLoading();
    try {
        const response = await fetch(`${API_BASE_URL}/brand`);
        const data = await response.json(); // API返回 [{name: '品牌A', value: 123}, ...]

        const option = {
            tooltip: { trigger: 'item', formatter: '{b}: ¥{c}' },
            series: [{
                type: 'treemap',
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
    