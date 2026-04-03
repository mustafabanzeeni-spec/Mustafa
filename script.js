// Elements
const loginForm = document.getElementById('loginForm');
const loginSection = document.getElementById('login-section');
const dashboardSection = document.getElementById('dashboard-section');
const userDisplay = document.getElementById('user-display');
const userNameDisplay = document.getElementById('user-name-display');
const sidebarItems = document.querySelectorAll('.sidebar ul li');
const notificationsPanel = document.getElementById('notifications-panel');
const addEntryForm = document.getElementById('addEntryForm');
const createUserForm = document.getElementById('createUserForm');
const userList = document.getElementById('userList');

// Sample Data
const entryData = [
  {product: 'Apples', category: 'Fruit', quantity: 50, revenue: 500, date: '2026-04-01'},
  {product: 'Oranges', category: 'Fruit', quantity: 75, revenue: 800, date: '2026-04-02'},
  {product: 'Bananas', category: 'Fruit', quantity: 100, revenue: 1200, date: '2026-04-03'},
  {product: 'Grapes', category: 'Fruit', quantity: 60, revenue: 700, date: '2026-04-04'},
  {product: 'Pears', category: 'Fruit', quantity: 40, revenue: 400, date: '2026-04-05'}
];

const users = [
  {username: 'Mustafa', password: '1234'}
];

let salesChart, trafficChart, revenueChart, analyticsChart;

// -------------------- Login --------------------
loginForm.addEventListener('submit', function(e){
  e.preventDefault();
  const username = document.getElementById('username').value.trim();
  const password = document.getElementById('password').value.trim();

  const user = users.find(u => u.username === username && u.password === password);
  if(user){
    loginSection.style.display = 'none';
    dashboardSection.classList.remove('dashboard-hidden');
    dashboardSection.style.display = 'flex';
    userDisplay.textContent = username;
    userNameDisplay.textContent = username;
    showPage('dashboard');
    renderDashboardCharts();
    renderEntryTable();
    updateDashboardWidgets();
    renderUserList();
  } else {
    alert('Invalid username or password!');
  }
});

if(addEntryForm){
  addEntryForm.addEventListener('submit', addEntry);
}
if(createUserForm){
  createUserForm.addEventListener('submit', createUser);
}
renderUserList();
updateDashboardWidgets();

// -------------------- Logout --------------------
function logout(){
  dashboardSection.classList.add('dashboard-hidden');
  dashboardSection.style.display = 'none';
  loginSection.style.display = 'block';
  loginForm.reset();
}

// -------------------- Page Navigation --------------------
function showPage(pageId){
  const pages = document.querySelectorAll('.page');
  pages.forEach(p => p.style.display = 'none');
  document.getElementById(pageId).style.display = 'block';

  // Active menu highlight
  sidebarItems.forEach(item => item.classList.remove('active'));
  document.querySelector(`.sidebar ul li[onclick="showPage('${pageId}')"]`).classList.add('active');
}

// -------------------- Charts --------------------
function renderDashboardCharts(){
  // Destroy existing charts before re-rendering
  if(salesChart) salesChart.destroy();
  if(trafficChart) trafficChart.destroy();
  if(revenueChart) revenueChart.destroy();
  if(analyticsChart) analyticsChart.destroy();

  const labels = entryData.map(d => d.product);
  const revenueValues = entryData.map(d => d.revenue);
  const quantityValues = entryData.map(d => d.quantity);
  const categoryTotals = entryData.reduce((acc, entry) => {
    acc[entry.category] = (acc[entry.category] || 0) + entry.revenue;
    return acc;
  }, {});

  // Revenue Bar Chart
  const salesCtx = document.getElementById('salesChart').getContext('2d');
  salesChart = new Chart(salesCtx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'Revenue',
        data: revenueValues,
        backgroundColor: ['#4CAF50', '#66bb6a', '#81c784', '#a5d6a7', '#c8e6c9']
      }]
    },
    options: { responsive:true, plugins:{ legend:{display:false} } }
  });

  // Quantity Line Chart
  const trafficCtx = document.getElementById('trafficChart').getContext('2d');
  trafficChart = new Chart(trafficCtx, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: 'Quantity',
        data: quantityValues,
        borderColor: '#4CAF50',
        backgroundColor: 'rgba(76,175,80,0.2)',
        tension: 0.3,
        fill: true
      }]
    },
    options: { responsive: true }
  });

  // Category Revenue Doughnut Chart
  const revenueCtx = document.getElementById('revenueChart').getContext('2d');
  revenueChart = new Chart(revenueCtx, {
    type: 'doughnut',
    data: {
      labels: Object.keys(categoryTotals),
      datasets: [{
        label: 'Category Revenue',
        data: Object.values(categoryTotals),
        backgroundColor: ['#5c6bc0', '#42a5f5', '#26c6da', '#ab47bc', '#ec407a']
      }]
    },
    options: { responsive: true }
  });

  // Analytics Radar Chart
  const analyticsCtx = document.getElementById('analyticsChart')?.getContext('2d');
  if(analyticsCtx){
    analyticsChart = new Chart(analyticsCtx, {
      type: 'radar',
      data: {
        labels: ['Entries', 'Revenue', 'Quantity', 'Categories', 'Users'],
        datasets: [{
          label: 'Dashboard Metrics',
          data: [entryData.length, revenueValues.reduce((a,b)=>a+b,0), quantityValues.reduce((a,b)=>a+b,0), Object.keys(categoryTotals).length, users.length],
          backgroundColor: 'rgba(76,175,80,0.2)',
          borderColor: '#4CAF50',
          pointBackgroundColor: '#4CAF50'
        }]
      },
      options: { responsive:true }
    });
  }
}

function addEntry(e){
  e.preventDefault();
  const product = document.getElementById('entryProduct').value.trim();
  const category = document.getElementById('entryCategory').value;
  const quantity = Number(document.getElementById('entryQuantity').value);
  const revenue = Number(document.getElementById('entryRevenue').value);
  const date = new Date().toISOString().split('T')[0];

  if(!product || !category || quantity < 1 || revenue < 0){
    alert('Please fill out all entry fields with valid values.');
    return;
  }

  entryData.push({ product, category, quantity, revenue, date });
  document.getElementById('entryProduct').value = '';
  document.getElementById('entryCategory').value = '';
  document.getElementById('entryQuantity').value = '';
  document.getElementById('entryRevenue').value = '';

  renderDashboardCharts();
  renderEntryTable();
  updateDashboardWidgets();
}

function createUser(e){
  e.preventDefault();
  const username = document.getElementById('newUsername').value.trim();
  const password = document.getElementById('newPassword').value.trim();

  if(!username || !password){
    alert('Enter both username and password.');
    return;
  }

  const exists = users.some(u => u.username.toLowerCase() === username.toLowerCase());
  if(exists){
    alert('That user already exists. Choose a different username.');
    return;
  }

  users.push({ username, password });
  document.getElementById('newUsername').value = '';
  document.getElementById('newPassword').value = '';
  renderUserList();
  updateDashboardWidgets();
  alert('New user created successfully.');
}

function renderUserList(){
  if(!userList) return;
  userList.innerHTML = '';
  users.forEach(user => {
    const li = document.createElement('li');
    li.textContent = user.username;
    userList.appendChild(li);
  });
}

function updateDashboardWidgets(){
  const widgetEntries = document.getElementById('widgetEntries');
  const widgetRevenue = document.getElementById('widgetRevenue');
  const widgetOrders = document.getElementById('widgetOrders');
  const widgetCategories = document.getElementById('widgetCategories');

  if(widgetEntries) widgetEntries.textContent = `Entries: ${entryData.length}`;
  if(widgetRevenue) widgetRevenue.textContent = `Revenue: $${entryData.reduce((sum, item) => sum + item.revenue, 0)}`;
  if(widgetOrders) widgetOrders.textContent = `Items: ${entryData.reduce((sum, item) => sum + item.quantity, 0)}`;
  if(widgetCategories) widgetCategories.textContent = `Categories: ${[...new Set(entryData.map(item => item.category))].length}`;
}

// -------------------- Entries Table --------------------
function renderEntryTable(){
  const tbody = document.querySelector('#entriesTable tbody');
  if(!tbody) return;
  tbody.innerHTML = '';
  entryData.slice().reverse().forEach(d => {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${d.product}</td><td>${d.category}</td><td>${d.quantity}</td><td>$${d.revenue}</td><td>${d.date}</td>`;
    tbody.appendChild(tr);
  });
}

function searchEntryTable(){
  const input = document.getElementById('entrySearch').value.toLowerCase();
  const rows = document.querySelectorAll('#entriesTable tbody tr');
  rows.forEach(row => {
    const cells = Array.from(row.cells).slice(0, 2).map(cell => cell.textContent.toLowerCase());
    row.style.display = cells.some(text => text.includes(input)) ? '' : 'none';
  });
}

// -------------------- Table Search --------------------
function searchTable(){
  const input = document.getElementById('tableSearch').value.toLowerCase();
  const rows = document.querySelectorAll('#salesTable tbody tr');
  rows.forEach(row => {
    const product = row.cells[0].textContent.toLowerCase();
    row.style.display = product.includes(input)? '' : 'none';
  });
}

// -------------------- Dark/Light Theme --------------------
function toggleTheme(){
  document.body.classList.toggle('dark-theme');
}

// -------------------- Collapsible Sidebar --------------------
function toggleSidebar(){
  document.querySelector('.sidebar').classList.toggle('collapsed');
}

// -------------------- Notifications Panel --------------------
function toggleNotifications(){
  notificationsPanel.style.display = notificationsPanel.style.display === 'block' ? 'none' : 'block';
}

// Close notifications when clicking outside
document.addEventListener('click', function(e){
  if(!notificationsPanel.contains(e.target) && !e.target.matches('.top-navbar button')){
    notificationsPanel.style.display = 'none';
  }
});