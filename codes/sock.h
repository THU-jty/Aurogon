#ifndef SOCK_H
#define SOCK_H

int sock_daemon_connect(
	int port);

int sock_client_connect(
	const char *server_name,
	int port);

int sock_sync_data(
	int sock_fd,
	int is_daemon,
	size_t size,
	const void *out_buf,
	void *in_buf);

int sock_sync_ready(
	int sock_fd,
	int is_daemon);

int sock_recv(
	int sock_fd,
	size_t size,
	void *buf);

int sock_send(
	int sock_fd,
	size_t size,
	const void *buf);


#endif /* SOCK_H */
