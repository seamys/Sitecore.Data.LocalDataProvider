using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using MahApps.Metro.Controls;
using MahApps.Metro.Controls.Dialogs;

namespace Sitecore.Data.LocalDataProvider.Tools
{
    /// <summary>
    /// MainWindow.xaml 的交互逻辑
    /// </summary>
    public partial class MainWindow : MetroWindow
    {
        public MainWindow()
        {
            InitializeComponent();
            QueryResult.Visibility = Visibility.Hidden;
        }

        public async Task SetQueryResult(string file)
        {
            if (!File.Exists(file))
                return;
            var table = new DataTable("QueryResult");
            try
            {
                using (StreamDataReader reader = new StreamDataReader(file))
                {
                    QueryText.Text = reader.QueryText;
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        table.Columns.Add(reader.GetName(i), reader.GetFieldType(i));
                    }
                    int count = 200;
                    while (reader.Read() && count-- > 0)
                    {
                        object[] rowObjects = new object[reader.FieldCount];
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            rowObjects[i] = reader[i];
                        }
                        table.Rows.Add(rowObjects);
                    }
                }
                QueryResult.Visibility = Visibility.Visible;
                QueryResult.ItemsSource = table.AsDataView();
            }
            catch (Exception e)
            {
                await this.ShowMessageAsync("File reading failed!", e.Message);
            }
        }

        private async void DataFileDrop(object sender, DragEventArgs e)
        {
            if (!e.Data.GetDataPresent(DataFormats.FileDrop)) return;
            string[] files = (string[])e.Data.GetData(DataFormats.FileDrop);
            if (files != null)
                await SetQueryResult(files[0]);
        }
    }
}
