using Amqp;
using Amqp.Framing;
using Amqp.Types;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices.WindowsRuntime;
using Windows.Foundation;
using Windows.Foundation.Collections;
using Windows.UI.Popups;
using Windows.UI.Xaml;
using Windows.UI.Xaml.Controls;
using Windows.UI.Xaml.Controls.Primitives;
using Windows.UI.Xaml.Data;
using Windows.UI.Xaml.Input;
using Windows.UI.Xaml.Media;
using Windows.UI.Xaml.Navigation;

// The Blank Page item template is documented at http://go.microsoft.com/fwlink/?LinkId=402352&clcid=0x409

namespace ReceivingData
{
    /// <summary>
    /// An empty page that can be used on its own or navigated to within a Frame.
    /// </summary>
    public sealed partial class MainPage : Page
    {
        const string sbNamespace = "iacaddemo.servicebus.windows.net";
        const string keyName = "Send";
        const string keyValue = "+k5+YyNNhlYWxW3mwuY7ANMO4inkwo07ECB3JB03si8=";

        const string ReckeyName = "Listen";
        const string ReckeyValue = "WRs9ta93rSpZN4EeHKKWFHYPKD6o99FwZJkRfwRV27U=";

        const string entity = "iacaddemo";

        public MainPage()
        {
            this.InitializeComponent();
        }

        private void ReceiveBtn_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                string[] partitions = GetPartitions();
                var checkpoint = ReceiveMessages("receive without filter", 200, null, partitions[1]);
                ReceiveMessages("receive from offset checkpoint", int.MaxValue, "amqp.annotation.x-opt-offset > '" + checkpoint.Item1 + "'", partitions[1]);
                long ticksOffset = (long)checkpoint.Item2.Subtract(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;
                ReceiveMessages("receive from time checkpoint", int.MaxValue, "amqp.annotation.x-opt-enqueuedtimeutc > " + ticksOffset, partitions[1]);
            }
            catch (Exception ex)
            {
                new MessageDialog(ex.Message).ShowAsync();
            }
            
        }

        string[] GetPartitions()
        {
            Address address = new Address(sbNamespace, 5671, ReckeyName, ReckeyValue);
            Connection connection = new Connection(address);
            Session session = new Session(connection);

            // create a pair of links for request/response
            string clientNode = "client-temp-node";
            SenderLink sender = new SenderLink(session, "mgmt-sender", "$management");
            ReceiverLink receiver = new ReceiverLink(
                session,
                "mgmt-receiver",
                new Attach()
                {
                    Source = new Source() { Address = "$management" },
                    Target = new Target() { Address = clientNode }
                },
                null);

            Message request = new Message();
            request.Properties = new Properties() { MessageId = "request1", ReplyTo = clientNode };
            request.ApplicationProperties = new ApplicationProperties();
            request.ApplicationProperties["operation"] = "READ";
            request.ApplicationProperties["name"] = entity;
            request.ApplicationProperties["type"] = "com.microsoft:eventhub";
            sender.Send(request, null, null);

            Message response = receiver.Receive();
            if (response == null)
            {
                throw new Exception("No response was received.");
            }
            receiver.Accept(response);
            receiver.Close();
            sender.Close();
            connection.Close();
            string[] partitions = (string[])((Map)response.Body)["partition_ids"];
            return partitions;
        }

        Tuple<string, DateTime> ReceiveMessages(string scenario, int count, string filter, string partition)
        {
            Address address = new Address(sbNamespace, 5671, ReckeyName, ReckeyValue);
            Connection connection = new Connection(address);
            Session session = new Session(connection);
            string partitionAddress = entity + "/ConsumerGroups/$default/Partitions/" + partition; ;
            Map filters = new Map();
            if (filter != null)
            {
                filters.Add(new Amqp.Types.Symbol("apache.org:selector-filter:string"),
                    new DescribedValue(new Amqp.Types.Symbol("apache.org:selector-filter:string"), filter));
            }

            string lastOffset = "-1";
            long lastSeqNumber = -1;
            DateTime lastEnqueueTime = DateTime.MinValue;

            ReceiverLink receiver = new ReceiverLink(
                session,
                "receiver-" + partition,
                new Source() { Address = partitionAddress, FilterSet = filters },
                null);

            int i;
            for (i = 0; i < count; i++)
            {
                Message message = receiver.Receive(30000);
                if (message == null)
                {
                    break;
                }

                receiver.Accept(message);
                lastOffset = (string)message.MessageAnnotations[new Amqp.Types.Symbol("x-opt-offset")];
                lastSeqNumber = (long)message.MessageAnnotations[new Amqp.Types.Symbol("x-opt-sequence-number")];
                lastEnqueueTime = (DateTime)message.MessageAnnotations[new Amqp.Types.Symbol("x-opt-enqueued-time")];
            }

            receiver.Close();
            session.Close();
            connection.Close();
            return Tuple.Create(lastOffset, lastEnqueueTime);
        }


    }
}
