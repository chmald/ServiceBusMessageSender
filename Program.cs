using Azure.Messaging.ServiceBus;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddSingleton<ServiceBusSender>(x =>
{
    var clientOptions = new ServiceBusClientOptions()
    {
        TransportType = ServiceBusTransportType.AmqpWebSockets
    };
    var client = new ServiceBusClient(builder.Configuration.GetValue<string>("ServiceBusConnectionString"), clientOptions);
    var sender = client.CreateSender(builder.Configuration.GetValue<string>("ServiceBusQueueName"));

    return sender;
});

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
